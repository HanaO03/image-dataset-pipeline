-- =============================================================================
--  Image Dataset Ingestion Pipeline — PostgreSQL schema
-- =============================================================================
--  Applied idempotently by the application on every startup (see src/db/connection.py).
--
--  Why applied from the app and not via docker-entrypoint-initdb.d:
--  entrypoint scripts only run when the PGDATA volume is EMPTY. Relying on them
--  alone means `docker compose up` breaks on the second run against an existing
--  volume. Applying the schema from the app makes both paths work.
--  In production this would be Alembic; a single idempotent DDL file is the
--  right weight for this scope.
--
--  Layering:  raw_records (raw) -> images (curated) -> exports (/data/output)
--  Run audit: pipeline_runs, run_stage_metrics, rejections
-- =============================================================================

-- pgcrypto gives us gen_random_uuid(); on PG >= 13 it is built in, but the
-- extension keeps this portable to older servers.
CREATE EXTENSION IF NOT EXISTS pgcrypto;


-- -----------------------------------------------------------------------------
-- 1. pipeline_runs — one row per pipeline execution.
--    This is the table a teammate queries first: "did last night's run work?"
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at      TIMESTAMPTZ,
    -- 'partial' = the run completed but a quality gate failed (e.g. a class
    -- came back under the minimum image count). Distinct from 'failed' on purpose:
    -- silent under-delivery is the failure mode that actually hurts downstream ML.
    status           TEXT        NOT NULL DEFAULT 'running'
                     CHECK (status IN ('running', 'success', 'partial', 'failed')),
    trigger          TEXT        NOT NULL DEFAULT 'cli',
    -- Full effective config for this run. Answers "what settings produced this
    -- dataset?" without needing the git checkout.
    config_snapshot  JSONB       NOT NULL DEFAULT '{}'::jsonb,
    git_commit       TEXT,
    error_message    TEXT,
    notes            TEXT
);

COMMENT ON TABLE  pipeline_runs IS 'One row per pipeline execution: status, timing, effective config.';
COMMENT ON COLUMN pipeline_runs.status IS 'running | success | partial (quality gate failed) | failed';

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started_at ON pipeline_runs (started_at DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status     ON pipeline_runs (status);


-- -----------------------------------------------------------------------------
-- 2. raw_records — append-only landing zone for upstream payloads.
--
--    Stored BEFORE any transformation. This is what makes the pipeline
--    replayable: normalisation logic can change and be re-applied without
--    re-hitting the API (which is rate limited) or re-scraping (which is fragile).
--    JSONB because upstream schemas are messy, differ per source, and we refuse
--    to lose fields we did not anticipate.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_records (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id       UUID        NOT NULL REFERENCES pipeline_runs (run_id) ON DELETE CASCADE,
    source       TEXT        NOT NULL CHECK (source IN ('openverse', 'wikimedia_commons')),
    -- Upstream's own identifier. Nullable because scraped pages do not always
    -- expose a stable id — we fall back to the image URL.
    source_id    TEXT,
    class_label  TEXT        NOT NULL,
    payload      JSONB       NOT NULL,
    fetched_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE raw_records IS 'Append-only raw upstream payloads. Enables re-normalisation without re-fetching.';

CREATE INDEX IF NOT EXISTS idx_raw_records_run_id ON raw_records (run_id);
CREATE INDEX IF NOT EXISTS idx_raw_records_source_class ON raw_records (source, class_label);
-- GIN index on the payload so ad-hoc questions ("which records had no license
-- field?") stay answerable without a migration.
CREATE INDEX IF NOT EXISTS idx_raw_records_payload ON raw_records USING GIN (payload);


-- -----------------------------------------------------------------------------
-- 3. images — the curated table. One row per DISTINCT image we accepted.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS images (
    id                 BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- Content identity ---------------------------------------------------------
    -- sha256 of the image BYTES. UNIQUE here, not de-duplicated in Python, so the
    -- invariant survives concurrent workers, crashes and re-runs. Combined with
    -- INSERT ... ON CONFLICT DO NOTHING this is what makes `docker compose up`
    -- safe to run twice.
    sha256             CHAR(64)    NOT NULL UNIQUE,
    -- 64-bit perceptual hash as 16 hex chars. Used for near-duplicate detection
    -- via Hamming distance. Stored as text for readability; see note below on
    -- what this becomes at scale.
    phash              CHAR(16),

    -- Label & provenance -------------------------------------------------------
    class_label        TEXT        NOT NULL,
    source             TEXT        NOT NULL CHECK (source IN ('openverse', 'wikimedia_commons')),
    source_id          TEXT        NOT NULL,
    source_url         TEXT,               -- landing page a human can open
    origin_url         TEXT        NOT NULL, -- direct URL the bytes came from

    -- Licensing ----------------------------------------------------------------
    -- NOT NULL by policy: an image without a determinable licence is rejected at
    -- validation, never stored. A training dataset with unknown provenance is a
    -- legal liability, so we make the schema enforce the policy rather than
    -- trusting application code to remember.
    license            TEXT        NOT NULL,
    license_url        TEXT,
    attribution        TEXT,

    -- File facts (measured from the downloaded bytes, never trusted from upstream)
    storage_path       TEXT        NOT NULL,
    format             TEXT        NOT NULL,
    width              INTEGER     NOT NULL CHECK (width  > 0),
    height             INTEGER     NOT NULL CHECK (height > 0),
    file_size_bytes    BIGINT      NOT NULL CHECK (file_size_bytes > 0),

    -- ML packaging -------------------------------------------------------------
    split              TEXT        CHECK (split IN ('train', 'val')),

    -- Deduplication ------------------------------------------------------------
    -- Near-duplicates are MARKED, not deleted. Preserves the audit trail so
    -- "why did this image disappear from the dataset?" stays answerable.
    duplicate_of       BIGINT      REFERENCES images (id) ON DELETE SET NULL,
    duplicate_distance SMALLINT,           -- Hamming distance to duplicate_of

    -- Lineage ------------------------------------------------------------------
    first_seen_run_id  UUID        REFERENCES pipeline_runs (run_id) ON DELETE SET NULL,
    last_seen_run_id   UUID        REFERENCES pipeline_runs (run_id) ON DELETE SET NULL,
    retrieved_at       TIMESTAMPTZ NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Natural key from upstream: the same asset should not be ingested twice
    -- under two rows even if the bytes were re-encoded between runs.
    CONSTRAINT uq_images_source_natural_key UNIQUE (source, source_id),
    -- A row cannot be its own duplicate.
    CONSTRAINT ck_images_not_self_duplicate CHECK (duplicate_of IS NULL OR duplicate_of <> id),
    -- distance is only meaningful when a duplicate is recorded.
    CONSTRAINT ck_images_duplicate_distance CHECK (
        (duplicate_of IS NULL AND duplicate_distance IS NULL)
        OR (duplicate_of IS NOT NULL AND duplicate_distance IS NOT NULL)
    )
);

COMMENT ON TABLE  images IS 'Curated, validated, deduplicated image records. One row per distinct image.';
COMMENT ON COLUMN images.sha256 IS 'sha256 of image bytes. UNIQUE — the exact-duplicate guarantee lives in the DB.';
COMMENT ON COLUMN images.phash IS '64-bit perceptual hash, 16 hex chars. Near-duplicate detection by Hamming distance.';
COMMENT ON COLUMN images.duplicate_of IS 'Set when this row is a near-duplicate of an earlier row. Marked, not deleted.';

-- Indexes. At 180 rows Postgres will seq-scan regardless; these exist because
-- the access patterns are known and would matter at real volume:
--   - the ML export reads by (class_label, split)
--   - dedup candidate lookup filters out already-marked duplicates
--   - run drill-down filters by first_seen_run_id
CREATE INDEX IF NOT EXISTS idx_images_class_split   ON images (class_label, split);
-- Looked up once per run to answer "have we already downloaded this URL?".
-- A hash cannot answer that question (it is only known after the transfer), so
-- this index is what makes a second `docker compose up` cost no bandwidth.
CREATE INDEX IF NOT EXISTS idx_images_origin_url    ON images (origin_url);
CREATE INDEX IF NOT EXISTS idx_images_phash         ON images (phash) WHERE duplicate_of IS NULL;
CREATE INDEX IF NOT EXISTS idx_images_first_run     ON images (first_seen_run_id);
CREATE INDEX IF NOT EXISTS idx_images_duplicate_of  ON images (duplicate_of);

-- NOTE ON SCALE: linear Hamming comparison over phash is O(n^2) per run and is
-- correct at this volume. Beyond ~1e5 images the answer is a BK-tree in the app,
-- or storing the hash as a bit vector in pgvector and using a HNSW index.


-- -----------------------------------------------------------------------------
-- 4. rejections — every item we did NOT keep, and why.
--
--    Deliberately a table and not a log line. The task asks for something a
--    teammate would query later; "grep the logs" is not that. Rejection rate by
--    reason_code, compared across runs, is the pipeline's health signal.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rejections (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id       UUID        NOT NULL REFERENCES pipeline_runs (run_id) ON DELETE CASCADE,
    stage        TEXT        NOT NULL,
    source       TEXT        NOT NULL,
    class_label  TEXT,
    source_id    TEXT,
    source_url   TEXT,
    -- Closed vocabulary, enforced by the app (see models.RejectionReason).
    -- Kept as TEXT + application enum rather than a Postgres ENUM: altering a PG
    -- ENUM is a migration-time headache, and a lookup table is over-engineering
    -- for a value set this small and this stable.
    reason_code  TEXT        NOT NULL,
    detail       TEXT,
    occurred_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE rejections IS 'Every discarded item with a machine-readable reason. The pipeline health signal.';

-- The stage vocabulary is applied as a named constraint that is dropped and
-- re-added on every startup, rather than inline in CREATE TABLE.
--
-- `CREATE TABLE IF NOT EXISTS` is a no-op against an existing database, so an
-- inline CHECK is frozen at whatever the table was first created with: adding a
-- stage to the application would then fail at INSERT time on every database
-- except a freshly created one — and only in production, never in a clean test.
-- Re-applying the constraint here keeps the DDL file the single source of truth.
ALTER TABLE rejections DROP CONSTRAINT IF EXISTS rejections_stage_check;
ALTER TABLE rejections ADD  CONSTRAINT rejections_stage_check
      CHECK (stage IN ('ingest', 'download', 'validate', 'dedupe', 'normalize', 'select'));

CREATE INDEX IF NOT EXISTS idx_rejections_run     ON rejections (run_id);
CREATE INDEX IF NOT EXISTS idx_rejections_reason  ON rejections (reason_code);
CREATE INDEX IF NOT EXISTS idx_rejections_run_stage ON rejections (run_id, stage);


-- -----------------------------------------------------------------------------
-- 5. run_stage_metrics — counters in long format.
--
--    Long (stage, metric, value) rather than wide columns: adding a new counter
--    is an INSERT, not an ALTER TABLE. Trades a little query verbosity for
--    schema stability — the right trade for a metrics table.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS run_stage_metrics (
    run_id  UUID   NOT NULL REFERENCES pipeline_runs (run_id) ON DELETE CASCADE,
    stage   TEXT   NOT NULL,
    metric  TEXT   NOT NULL,
    value   BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, stage, metric)
);

COMMENT ON TABLE run_stage_metrics IS 'Long-format per-stage counters: fetched, downloaded, rejected, deduped, kept.';


-- -----------------------------------------------------------------------------
-- 6. run_summary — the one-query answer for a teammate.
-- -----------------------------------------------------------------------------
-- Dropped and recreated rather than CREATE OR REPLACE: replacing a view can
-- only *append* columns, so inserting one in the middle fails against an
-- existing database with "cannot change name of view column". A view holds no
-- data, so dropping it costs nothing.
DROP VIEW IF EXISTS run_summary;
CREATE VIEW run_summary AS
SELECT
    r.run_id,
    r.status,
    r.started_at,
    r.finished_at,
    round(EXTRACT(EPOCH FROM (COALESCE(r.finished_at, now()) - r.started_at))::numeric, 1)
        AS duration_seconds,
    COALESCE(m.fetched, 0)     AS fetched,
    COALESCE(m.downloaded, 0)  AS downloaded,
    COALESCE(m.kept, 0)        AS kept,
    COALESCE(rj.rejected, 0)   AS rejected,
    COALESCE(m.near_dupes, 0)  AS near_duplicates_marked,
    COALESCE(rj.trimmed, 0)    AS trimmed_over_target,
    CASE
        WHEN COALESCE(m.fetched, 0) = 0 THEN NULL
        ELSE round(100.0 * COALESCE(rj.rejected, 0) / m.fetched, 1)
    END AS rejection_rate_pct,
    r.error_message
FROM pipeline_runs r
LEFT JOIN (
    SELECT run_id,
           SUM(value) FILTER (WHERE metric = 'fetched')             AS fetched,
           SUM(value) FILTER (WHERE metric = 'downloaded')          AS downloaded,
           SUM(value) FILTER (WHERE metric = 'kept')                AS kept,
           -- Counted from this run's own metrics, not from images.duplicate_of.
           -- A row is attributed to the run that FIRST saw the image, so
           -- counting the table would credit a marking made this week to the
           -- run that collected the image last month.
           SUM(value) FILTER (WHERE metric = 'near_duplicates')     AS near_dupes
    FROM run_stage_metrics
    GROUP BY run_id
) m ON m.run_id = r.run_id
LEFT JOIN (
    -- OVER_TARGET is a capacity decision, not a defect: the image was valid,
    -- licensed and unique, and its class was simply full. Counting it as a
    -- rejection would put a healthy run at a ~40% rejection rate and destroy
    -- the one number this view exists to make comparable across runs.
    SELECT run_id,
           COUNT(*) FILTER (WHERE reason_code <> 'OVER_TARGET') AS rejected,
           COUNT(*) FILTER (WHERE reason_code =  'OVER_TARGET') AS trimmed
    FROM rejections GROUP BY run_id
) rj ON rj.run_id = r.run_id;

COMMENT ON VIEW run_summary IS 'One row per run: counts, duration, rejection rate. Start here when debugging a run.';


-- -----------------------------------------------------------------------------
-- 7. dataset_composition — class/split balance of the current curated dataset.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW dataset_composition AS
SELECT
    class_label,
    split,
    COUNT(*)                AS n_images,
    ROUND(AVG(width))       AS avg_width,
    ROUND(AVG(height))      AS avg_height,
    COUNT(DISTINCT source)  AS n_sources
FROM images
WHERE duplicate_of IS NULL
  AND split IS NOT NULL
GROUP BY class_label, split
ORDER BY class_label, split;

COMMENT ON VIEW dataset_composition IS 'Class/split balance of the exportable dataset. Detects imbalance at a glance.';


-- -----------------------------------------------------------------------------
-- 8. updated_at maintenance
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create the trigger only when it is genuinely absent.
--
-- The obvious `DROP TRIGGER IF EXISTS` + `CREATE TRIGGER` pair is a trap here:
-- both statements take an ACCESS EXCLUSIVE lock on `images`, so applying the
-- schema at startup would block behind ANY other session holding even a read
-- lock — a second pipeline container starting concurrently, or a reviewer who
-- left a psql session open mid-transaction. The result is a run that hangs
-- forever with no error, which is the worst possible failure mode.
--
-- Checking pg_trigger first takes no lock on the table at all, so the steady
-- state (trigger already present) costs nothing and cannot block.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
         WHERE tgname = 'trg_images_updated_at'
           AND tgrelid = 'images'::regclass
           AND NOT tgisinternal
    ) THEN
        CREATE TRIGGER trg_images_updated_at
            BEFORE UPDATE ON images
            FOR EACH ROW EXECUTE FUNCTION set_updated_at();
    END IF;
END
$$;

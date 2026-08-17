-- =============================================================================
--  Schema smoke test — proves the guarantees the schema claims to provide.
--
--  Run it against any database the schema has been applied to, including one
--  already holding a real dataset:
--      psql -d imagedb -f sql/verify_schema.sql
--
--  Safe on live data: every assertion measures a *delta* against a baseline
--  captured at the start, and the whole thing ends in ROLLBACK. Asserting
--  absolute counts instead would be simpler and would fail the moment anyone
--  ran it after `docker compose up` — which is exactly when a reviewer will.
--
--  Every check prints PASS or FAIL. Nothing here depends on Python, so the
--  data model can be reviewed and trusted independently of the application.
-- =============================================================================

BEGIN;

\set QUIET on
\set ON_ERROR_STOP off

CREATE TEMP TABLE results (check_name TEXT, outcome TEXT);

-- Helper: record whether a statement raised the expected constraint violation.
CREATE OR REPLACE FUNCTION expect_violation(stmt TEXT, expected_constraint TEXT, label TEXT)
RETURNS VOID AS $$
BEGIN
    BEGIN
        EXECUTE stmt;
        INSERT INTO results VALUES (label, 'FAIL — statement was allowed');
    EXCEPTION WHEN OTHERS THEN
        IF POSITION(expected_constraint IN SQLERRM) > 0 THEN
            INSERT INTO results VALUES (label, 'PASS');
        ELSE
            INSERT INTO results VALUES (label, 'FAIL — wrong error: ' || SQLERRM);
        END IF;
    END;
END;
$$ LANGUAGE plpgsql;


-- Baseline: whatever the database already holds. Every count below is compared
-- against this, so the test is about what these statements changed, not about
-- the database happening to be empty.
CREATE TEMP TABLE baseline AS
SELECT (SELECT count(*) FROM images)            AS images,
       (SELECT count(*) FROM rejections)        AS rejections,
       (SELECT count(*) FROM run_stage_metrics) AS metrics,
       (SELECT COALESCE(sum(n_images), 0) FROM dataset_composition) AS composition;


-- -----------------------------------------------------------------------------
-- Fixture
-- -----------------------------------------------------------------------------
INSERT INTO pipeline_runs (run_id, status, config_snapshot)
VALUES ('11111111-1111-1111-1111-111111111111', 'running',
        '{"classes": ["cat","dog","bird"]}'::jsonb);

INSERT INTO images (sha256, phash, class_label, source, source_id, origin_url,
                    license, storage_path, format, width, height,
                    file_size_bytes, retrieved_at, first_seen_run_id, split)
VALUES
 (repeat('a',64), 'ffee001122334455', 'cat', 'openverse',         'ov-1',
  'http://example.org/1.jpg', 'CC-BY-4.0',    '/data/images/aa/a.jpg', 'JPEG',
  800, 600, 120000, now(), '11111111-1111-1111-1111-111111111111', 'train'),
 (repeat('b',64), 'ffee001122334457', 'cat', 'openverse',         'ov-2',
  'http://example.org/2.jpg', 'CC0-1.0',      '/data/images/bb/b.jpg', 'JPEG',
  640, 480,  90000, now(), '11111111-1111-1111-1111-111111111111', 'train'),
 (repeat('c',64), '0000111122223333', 'dog', 'wikimedia_commons', 'wc-1',
  'http://example.org/3.png', 'CC-BY-SA-3.0', '/data/images/cc/c.png', 'PNG',
  1024, 768, 300000, now(), '11111111-1111-1111-1111-111111111111', 'val');


-- -----------------------------------------------------------------------------
-- 1. IDEMPOTENCY — the central guarantee. Re-running must not duplicate rows.
-- -----------------------------------------------------------------------------
INSERT INTO images (sha256, class_label, source, source_id, origin_url, license,
                    storage_path, format, width, height, file_size_bytes, retrieved_at)
VALUES (repeat('a',64), 'cat', 'openverse', 'ov-1', 'http://example.org/1.jpg',
        'CC-BY-4.0', '/data/images/aa/a.jpg', 'JPEG', 800, 600, 120000, now())
ON CONFLICT (sha256) DO NOTHING;

INSERT INTO results
SELECT 'idempotent re-insert (same image, same run twice)',
       CASE WHEN count(*) - (SELECT images FROM baseline) = 3
            THEN 'PASS'
            ELSE 'FAIL — added ' || (count(*) - (SELECT images FROM baseline)) END
FROM images;

-- Same bytes arriving from the OTHER source must also collapse to one row.
INSERT INTO images (sha256, class_label, source, source_id, origin_url, license,
                    storage_path, format, width, height, file_size_bytes, retrieved_at)
VALUES (repeat('a',64), 'cat', 'wikimedia_commons', 'wc-99', 'http://other.org/9.jpg',
        'CC0-1.0', '/data/images/aa/a.jpg', 'JPEG', 800, 600, 120000, now())
ON CONFLICT (sha256) DO NOTHING;

INSERT INTO results
SELECT 'cross-source exact duplicate collapses to one row',
       CASE WHEN count(*) - (SELECT images FROM baseline) = 3
            THEN 'PASS'
            ELSE 'FAIL — added ' || (count(*) - (SELECT images FROM baseline)) END
FROM images;


-- -----------------------------------------------------------------------------
-- 2. CONSTRAINTS — bad data must be impossible, not merely discouraged.
-- -----------------------------------------------------------------------------
SELECT expect_violation(
    $$UPDATE images SET duplicate_of = id, duplicate_distance = 0
       WHERE sha256 = repeat('c',64)$$,
    'ck_images_not_self_duplicate',
    'reject: image marked as its own duplicate');

SELECT expect_violation(
    $$UPDATE images SET duplicate_of = 1 WHERE sha256 = repeat('b',64)$$,
    'ck_images_duplicate_distance',
    'reject: duplicate_of without duplicate_distance');

SELECT expect_violation(
    $$INSERT INTO images (sha256,class_label,source,source_id,origin_url,license,
                          storage_path,format,width,height,file_size_bytes,retrieved_at)
      VALUES (repeat('z',64),'cat','flickr','f1','u','CC0','p','JPEG',1,1,1,now())$$,
    'images_source_check',
    'reject: unknown source name');

SELECT expect_violation(
    $$UPDATE images SET split = 'test' WHERE sha256 = repeat('b',64)$$,
    'images_split_check',
    'reject: split outside {train, val}');

SELECT expect_violation(
    $$INSERT INTO images (sha256,class_label,source,source_id,origin_url,license,
                          storage_path,format,width,height,file_size_bytes,retrieved_at)
      VALUES (repeat('y',64),'cat','openverse','ov-y','u','CC0','p','JPEG',0,100,1,now())$$,
    'images_width_check',
    'reject: non-positive width');

SELECT expect_violation(
    $$INSERT INTO images (sha256,class_label,source,source_id,origin_url,
                          storage_path,format,width,height,file_size_bytes,retrieved_at)
      VALUES (repeat('x',64),'cat','openverse','ov-x','u','p','JPEG',10,10,1,now())$$,
    'null value in column "license"',
    'reject: image with no licence (strict licence policy)');


-- -----------------------------------------------------------------------------
-- 3. NEAR-DUPLICATE MARKING — the legitimate path must work.
-- -----------------------------------------------------------------------------
UPDATE images
   SET duplicate_of = (SELECT id FROM images WHERE sha256 = repeat('a',64)),
       duplicate_distance = 2,
       split = NULL
 WHERE sha256 = repeat('b',64);

INSERT INTO results
SELECT 'near-duplicate marked (not deleted), distance recorded',
       CASE WHEN duplicate_of IS NOT NULL AND duplicate_distance = 2
            THEN 'PASS' ELSE 'FAIL' END
FROM images WHERE sha256 = repeat('b',64);

INSERT INTO results
SELECT 'duplicates excluded from dataset_composition',
       CASE WHEN COALESCE(sum(n_images), 0) - (SELECT composition FROM baseline) = 2
            THEN 'PASS'
            ELSE 'FAIL — added '
                 || (COALESCE(sum(n_images), 0) - (SELECT composition FROM baseline)) END
FROM dataset_composition;


-- -----------------------------------------------------------------------------
-- 4. OBSERVABILITY — run_summary must answer "how did that run go?"
-- -----------------------------------------------------------------------------
INSERT INTO run_stage_metrics VALUES
 ('11111111-1111-1111-1111-111111111111','ingest',   'fetched',    12),
 ('11111111-1111-1111-1111-111111111111','download', 'downloaded',  9),
 ('11111111-1111-1111-1111-111111111111','normalize','kept',        3);

INSERT INTO rejections (run_id, stage, source, class_label, reason_code, detail) VALUES
 ('11111111-1111-1111-1111-111111111111','validate','openverse','cat','EXTENSION_MISMATCH','url said .jpg, bytes were PNG'),
 ('11111111-1111-1111-1111-111111111111','validate','openverse','bird','DIMENSIONS_TOO_SMALL','32x32'),
 ('11111111-1111-1111-1111-111111111111','ingest','wikimedia_commons','dog','MISSING_LICENSE',NULL);

INSERT INTO results
SELECT 'run_summary computes rejection rate',
       CASE WHEN rejection_rate_pct = 25.0 THEN 'PASS'
            ELSE 'FAIL — got ' || COALESCE(rejection_rate_pct::text,'null') END
FROM run_summary WHERE run_id = '11111111-1111-1111-1111-111111111111';


-- -----------------------------------------------------------------------------
-- 5. CASCADE — deleting a run must not orphan its audit rows.
-- -----------------------------------------------------------------------------
DELETE FROM pipeline_runs WHERE run_id = '11111111-1111-1111-1111-111111111111';

INSERT INTO results
SELECT 'run delete cascades to rejections/metrics, images survive',
       CASE WHEN (SELECT count(*) FROM rejections)        = (SELECT rejections FROM baseline)
             AND (SELECT count(*) FROM run_stage_metrics) = (SELECT metrics    FROM baseline)
             -- images are NOT cascaded: first_seen_run_id is ON DELETE SET NULL,
             -- because losing the audit row must never lose the data itself.
             AND (SELECT count(*) FROM images) = (SELECT images FROM baseline) + 3
            THEN 'PASS' ELSE 'FAIL' END;


-- -----------------------------------------------------------------------------
-- Report
-- -----------------------------------------------------------------------------
\set QUIET off
\echo ''
\echo '================ SCHEMA VERIFICATION ================'
SELECT check_name, outcome FROM results ORDER BY ctid;
SELECT CASE WHEN count(*) FILTER (WHERE outcome <> 'PASS') = 0
            THEN 'ALL CHECKS PASSED (' || count(*) || ')'
            ELSE count(*) FILTER (WHERE outcome <> 'PASS') || ' CHECK(S) FAILED'
       END AS verdict
FROM results;

ROLLBACK;   -- the smoke test leaves no trace

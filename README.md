# Image Dataset Ingestion Pipeline

Collects images of **cat / dog / bird** from a public API and by scraping a public
web page, validates and deduplicates them, stores the records and full run
metadata in PostgreSQL, and exports an ML-ready dataset to `/data/output`.

```bash
cp .env.example .env
docker compose up --build
```

That is the whole setup. It builds the image, starts Postgres, applies the
schema, runs the pipeline end to end, writes the dataset to `./data/output`, and
exits 0. **Running it a second time inserts nothing and downloads nothing** —
see [Idempotency](#idempotency).

---

## Contents

- [Architecture](#architecture)
- [What each module does](#what-each-module-does)
- [Database design](#database-design)
- [Idempotency](#idempotency)
- [Handling messy data](#handling-messy-data)
- [Key decisions and why](#key-decisions-and-why)
- [Running and inspecting](#running-and-inspecting)
- [Testing](#testing)
- [Reproducibility](#reproducibility)
- [What I would do with more time](#what-i-would-do-with-more-time)

---

## Architecture

```
                       ┌──────────────────────────────────────┐
                       │  runner (src/pipeline/run.py)        │
                       │  owns run_id, stage order, commits   │
                       └──────────────────────────────────────┘
                                        │
  ┌─────────────────────────────────────┼─────────────────────────────────────┐
  │                                     ▼                                     │
  │  1. INGEST      Openverse API  ──┐   scoped to what each class still needs │
  │                 Commons scrape ──┴─►  raw_records (JSONB, in Postgres)     │
  │                                                                            │
  │  2. DOWNLOAD    stream + sha256 ────►  /data/images/<sha[:2]>/<sha>.<ext>   │
  │                 known URL? reuse the stored bytes, no request              │
  │                                                                            │
  │  3. VALIDATE    decode twice, format vs extension, geometry, bombs         │
  │                            └──────►  rejections (reason_code)              │
  │                                                                            │
  │  4. DEDUPE      exact: UNIQUE(sha256)  ·  near: pHash + Hamming ≤ 5        │
  │                                                                            │
  │  5. NORMALIZE   licence → SPDX, one schema across both sources             │
  │                                                                            │
  │  6. SELECT      trim each class back to the target (60)                    │
  │                            └──────►  images (curated)                      │
  │                                                                            │
  │  7. SPLIT       stratified, deterministic, content-hash derived            │
  │                                                                            │
  │  8. EXPORT      dataset.parquet · dataset.csv · manifest.json              │
  │                 images/<split>/<class>/… · ATTRIBUTIONS.txt                │
  └────────────────────────────────────────────────────────────────────────────┘
```

Every stage has the same shape — `(records, settings) -> (kept, rejections,
metrics)` — and none of them import the database or know what runs before or
after. The runner is the only module that knows the order.

That is not decoration. It is what makes each stage unit-testable in isolation,
and it means replacing the runner with an Airflow DAG later is a change to one
file rather than a rewrite: each `_stage_*` method is already shaped like a task.

---

## What each module does

```
src/
├── config.py            every tunable, validated at import, env-overridable
├── models.py            data contracts + the closed vocabulary of reason codes
├── logging_setup.py     JSON logs; run_id and stage injected via contextvar
│
├── db/
│   ├── connection.py    wait for Postgres · apply schema idempotently
│   ├── repository.py    ★ every SQL statement in the project lives here
│   └── (sql/schema.sql) the DDL itself
│
├── http/client.py       one outbound path: UA, rate limit, backoff, robots.txt
│
├── sources/
│   ├── base.py          ImageSource — the extension point
│   ├── openverse.py     API adapter (optional OAuth for higher rate limits)
│   └── wikimedia.py     HTML scraper (category page → file page → licence)
│
├── pipeline/
│   ├── download.py      content-addressed storage, streaming sha256
│   ├── validate.py      ★ pure functions — the most heavily tested module
│   ├── dedupe.py        perceptual hashing + Hamming search
│   ├── normalize.py     licence → SPDX; the strict-licence policy
│   ├── select.py        ★ pure — holds each class to its target size
│   ├── split.py         ★ pure, deterministic, content-derived
│   ├── export.py        parquet / csv / manifest / attributions
│   └── run.py           ★ the orchestrator
│
└── cli.py               argument parsing and reporting only
```

**The one rule worth stating explicitly:** no module outside `db/` writes SQL,
and no module inside `pipeline/` touches the database. A reviewer can audit the
entire data-access surface by reading one file, and the pipeline stages can be
tested without Postgres running.

---

## Database design

Five tables and two views.

| table | purpose |
|---|---|
| `pipeline_runs` | one row per execution: status, timing, config snapshot, git commit |
| `raw_records` | append-only upstream payloads (JSONB), landed before interpretation — the replay layer lives in Postgres, not on disk |
| `images` | the curated layer: validated, deduplicated, split-assigned |
| `rejections` | every discarded item with a machine-readable `reason_code` |
| `run_stage_metrics` | long-format counters `(run_id, stage, metric, value)` |
| `run_summary` (view) | one row per run: counts, duration, rejection rate |
| `dataset_composition` (view) | class/split balance of the exportable dataset |

### Choices worth defending

**`sha256 CHAR(64) UNIQUE` rather than de-duplicating in Python.**
The database enforces the invariant unconditionally — across concurrent
workers, crashes, and re-runs. A Python-side check is only as good as our
control flow.

**Long-format metrics instead of wide columns.**
Adding a counter is an `INSERT`, not an `ALTER TABLE`. Slightly more verbose to
query; far more stable to evolve.

**`CHECK` + `TEXT` instead of a Postgres `ENUM`.**
Altering a PG enum is a migration-time headache. A `CHECK` constraint gives the
same guarantee and can be changed in place. A lookup table would be
over-engineering for a value set this small and this stable.

**`license NOT NULL`.**
The strict licence policy is enforced by the schema, not by application code
remembering to check. An image whose licence we cannot determine is rejected at
ingest and never reaches this table.

**Near-duplicates are marked, not deleted.**
`duplicate_of` is a self-referencing FK with a companion `duplicate_distance`,
and two CHECK constraints make an inconsistent marking impossible. The row stays
queryable, so *"why is this image not in the dataset?"* always has an answer.

**Indexes exist for access patterns, not for row counts.**
At 180 rows Postgres will sequential-scan regardless. `(class_label, split)`,
`phash` and `rejections(run_id)` are indexed because those are the queries this
system actually runs, and because the reasoning should be visible.

**Schema applied by the app, not `docker-entrypoint-initdb.d`.**
Entrypoint scripts only execute when the PGDATA volume is *empty*. A project
relying on them works exactly once and then breaks on every subsequent
`docker compose up` against the existing volume. `CREATE TABLE IF NOT EXISTS`
applied from the app behaves identically on the first run and the hundredth.
In production this would be Alembic.

**`lock_timeout` on schema application.**
DDL can require an `ACCESS EXCLUSIVE` lock. Without a timeout, a blocked
statement waits forever — a run that appears frozen with nothing in the logs.
One line converts that into a clear error in ten seconds. (The `updated_at`
trigger is created via a `pg_trigger` existence check rather than
`DROP … CREATE`, so the steady state takes no table lock at all.)

---

## Idempotency

The property the whole design is organised around: **`docker compose up` twice
produces exactly the same dataset, inserts no duplicate rows, and re-downloads
nothing.**

Four mechanisms, at four layers:

| layer | mechanism |
|---|---|
| source | ingestion is scoped to what each class still *needs* — a full class is not fetched at all |
| network | `origin_url` index: a URL already downloaded, whose file is still in the store, is reused without a request |
| database | `INSERT … ON CONFLICT (sha256) DO UPDATE` — the second run touches rows instead of creating them |
| dataset | split assignment derived from each image's own hash — no seed, no ordering dependency |

**Why the network layer needs its own key.** The content hash is the obvious
idempotency key and cannot do this job: it is only knowable *after* the bytes
have arrived, so it can report that a run fetched nothing new but cannot prevent
the transfer. The URL is known beforehand. Hashes still guard correctness — every
reused file is re-validated and re-hashed, and `images.sha256` stays the unique
key — while the URL index guards cost. The assumption it trades on is that a URL
still serves the bytes it served last time: true of both sources here (immutable
asset URLs), not true of the web in general, so `--refetch` forces the download
and a conditional `If-None-Match` is the stricter version.

Verified end to end in `tests/test_integration.py`, which runs the real pipeline
twice and asserts row count unchanged, `inserted == 0`, and an identical dataset
fingerprint — and confirmed against the live sources, below.

---

## Handling messy data

Real sources are messy, and the brief asks for that to be handled rather than
sanded away. Every one of these was encountered or deliberately reproduced, and
each has a specific `reason_code` and a test:

| what goes wrong | how it is caught | reason code |
|---|---|---|
| dead link returns HTTP 200 with an HTML error page | `Content-Type` check, then decoding the bytes | `NOT_AN_IMAGE` / `UNREADABLE_IMAGE` |
| download truncated mid-transfer | **decode the pixels**, not just `verify()` | `TRUNCATED_IMAGE` |
| `.jpg` URL serving PNG bytes | Pillow's detected format vs the URL extension | `EXTENSION_MISMATCH` |
| result has no direct image URL | checked at ingest, before spending a download | `MISSING_IMAGE_URL` |
| no licence, or one we cannot map | strict policy — rejected, never stored | `MISSING_LICENSE` / `UNRECOGNISED_LICENSE` |
| 1×1 tracking pixels, favicons | dimension and file-size floors | `DIMENSIONS_TOO_SMALL` |
| banners, spritesheets, diagrams | aspect-ratio bound (> 6:1) | `ASPECT_RATIO_EXTREME` |
| decompression bombs | `Image.MAX_IMAGE_PIXELS` | `IMAGE_BOMB` |
| same image from both sources | `sha256` unique constraint | `EXACT_DUPLICATE` |
| resized / re-compressed copy | pHash, Hamming ≤ 5 | `NEAR_DUPLICATE` |
| rate limiting (429) | backoff with jitter, `Retry-After` honoured | `HTTP_ERROR` |
| Commons changes its HTML | fallback selectors; parse failure ≠ crash | `PARSE_ERROR` |

> **The `verify()` trap.** Pillow's `Image.verify()` checks structure without
> decoding pixel data, so a file truncated halfway through passes it and then
> explodes inside the training loop instead. The only reliable test is to decode
> — and `verify()` invalidates the file object, so that means opening the file a
> second time. `tests/test_validate.py` asserts both halves of this: that
> `verify()` really does accept the truncated fixture, and that our validator
> rejects it anyway.

### Rejections are a table, not a log line

`grep the logs` is not something a teammate can query. Rejection counts grouped
by `reason_code` and compared across runs are how you notice that a source
changed its markup or started serving WebP:

```sql
SELECT stage, reason_code, count(*)
  FROM rejections WHERE run_id = '…'
 GROUP BY 1,2 ORDER BY 3 DESC;
```

### Licence policy, and why `all-cc` is the wrong filter

The first real run used Openverse's `license_type=all-cc` and produced this
breakdown:

```
CC-BY-NC-ND-2.0  13   CC-BY-2.0        9   CC-BY-NC-SA-2.0  8
CC-BY-NC-2.0      6   CC-BY-SA-2.0     6   CC0-1.0          4   CC-BY-ND-2.0  2
```

29 of 48 images carried **NC** (NonCommercial) or **ND** (NoDerivatives) terms.
ND is the serious one: a model trained on an image is arguably a derivative
work, so ND images are not safely usable as training data at all. NC rules the
rest out for any commercial product.

The dataset was *correct* — every licence was captured and normalised
accurately — and simultaneously *unusable*, which is the more interesting kind
of failure. The filter is now `license_type=commercial,modification`, applied
at the source so the images are never fetched in the first place.

Verify it on any run:

```bash
python -c "import json; print(json.load(open('data/output/manifest.json'))['licenses'])"
```

### Error policy

- **fail-soft on data** — a broken link, an unreadable JPEG, an unmappable
  licence: recorded with a reason code, the run continues.
- **fail-fast on infrastructure** — no database, unwritable volume, invalid
  config: the run stops, is marked `failed`, and says why.
- **quality gate in between** — the run completed but a class came in under
  `SOURCE_MIN_PER_CLASS`: status `partial`, exit code 2. Not a crash, and
  emphatically not a success. A pipeline that silently under-delivers a class
  produces a model that cannot recognise it, and nobody finds out until much
  later.

---

## Key decisions and why

| decision | alternative considered | why this one |
|---|---|---|
| **Openverse** as the API source | iNaturalist, Unsplash | returns `license`, `license_version`, `license_url` and a ready-made attribution string on every result. For a *training* dataset, licensing is not an afterthought. No auth required. |
| **Commons file pages** scraped as HTML | the Commons API | the brief asks for scraping a real page. Commons is also legal to scrape and states its licensing explicitly. |
| **files on disk, path in Postgres** | `BYTEA` in Postgres | keeps the DB small and fast to back up; image bytes are an object-storage concern, and this layout maps directly onto an S3 prefix later. |
| **content-addressed storage** | `<class>/<id>.jpg` | exact dedup becomes structural, re-runs skip downloads, and writes are atomic (temp file + rename after hashing). |
| **pHash + Hamming ≤ 5** | dHash, aHash, embeddings | pHash (DCT-based) is the most robust of the cheap hashes to re-compression and resizing. 5 is the conventional operating point; 6–10 is where false positives begin, and discarding real data is worse than keeping a rare near-miss. Embeddings would be better and cost a model dependency. |
| **dedup before split** | split then dedup | a near-duplicate straddling the boundary makes validation accuracy a lie. This is the single most important correctness issue in a small image dataset. |
| **content-hash split** | `train_test_split(random_state=42)` | a seeded shuffle is reproducible only for a *fixed* set — add ten images and everything can move. Hash-derived assignment has no seed and no ordering dependency. Two strategies are provided; see `pipeline/split.py` for the exact trade-off. |
| **Parquet primary, CSV secondary** | CSV only | CSV loses every type, needs careful quoting for the unicode in attribution strings, and is larger. CSV ships anyway because being able to eyeball the output has real value. |
| **plain Python + CLI** | Airflow / Prefect / Dagster | `docker compose up` must run end to end — an explicit requirement. An orchestrator inside compose adds significant startup risk for no benefit at this scale. Each stage is already shaped like a task, so adopting one later is mechanical. |
| **strict licence policy** | store with `license='UNKNOWN'` | the output trains a shipped model. Unknown provenance is a legal liability far more expensive than the handful of images it costs. |
| **threads, not processes** | multiprocessing | the work is I/O-bound. Processes would add overhead and no throughput. |

---

## Running and inspecting

```bash
make up             # build + run everything (the main entry point)
make smoke          # quick run, 10 images per class
make status         # recent runs + current dataset composition
make psql           # psql shell against the pipeline database
make verify-schema  # 12 assertions against the live schema, then rolls back
make test           # 103 tests in Docker — no local Python needed
make clean          # drop the volume and all outputs — start from nothing
```

Exit codes: `0` success · `1` failed · `2` partial (quality gate not met).

Useful queries once it has run:

```sql
SELECT * FROM run_summary ORDER BY started_at DESC LIMIT 5;
SELECT * FROM dataset_composition;
SELECT reason_code, count(*) FROM rejections GROUP BY 1 ORDER BY 2 DESC;

-- which images were dropped as near-duplicates, and of what?
SELECT d.sha256, o.sha256 AS duplicate_of, d.duplicate_distance
  FROM images d JOIN images o ON o.id = d.duplicate_of;
```

### Output

```
data/output/
├── dataset.parquet          typed, compressed — the primary artefact
├── dataset.csv              same rows, for human inspection
├── manifest.json            what a training script loads
├── ATTRIBUTIONS.txt         credit lines (a CC-BY licence obligation)
├── sample_images/           24 real images, committed — see below
└── images/<split>/<class>/<sha256>.<ext>
```

**What is in the repository and what is not.** The parquet, the CSV, the
manifest and the attributions are committed — they are the deliverable, and they
describe all 180 images. The 180 image *files* are not: at ~180 MB they would
make the clone hostile, and `make up` regenerates them exactly (same bytes, same
paths — that is what content-addressed storage means). So that the data can
still be eyeballed without running anything, `data/output/sample_images/` holds
24 real photographs, four per class and split, chosen deterministically by
`scripts/make_sample.py`.

This means the `path` field in `manifest.json` resolves after a run, not after a
clone. Documented rather than papered over: a manifest that pointed at a
committed subset would describe a dataset that does not exist.

`manifest.json` carries `schema_version`, `dataset_fingerprint`, `run_id`,
per-class/split counts, a licence breakdown, the producing configuration
(secrets stripped), and one entry per image with a relative path and checksum.
Paths are relative so the folder can be copied anywhere and still resolve.

### Rate limits

Openverse works anonymously, which is why no signup is needed to run this.
If you hit rate limiting, free client credentials raise the ceiling:

```bash
SOURCE_OPENVERSE_CLIENT_ID=…  SOURCE_OPENVERSE_CLIENT_SECRET=…  docker compose up
```

The adapter authenticates automatically when they are present and silently
falls back to anonymous when they are not — an auth failure is a performance
problem, not a reason to refuse to run.

---

## Results from the delivered run

`docker compose up` against a clean database, 2026-08-17. Run
`48f4d178-7eed-4154-a1fc-94645095148b`, exit code 0, `status=SUCCESS`, 90 seconds.

```
ingest      fetched=321   openverse=270   wikimedia_commons=51   sources_empty=0
download    downloaded=319   failed=2
validate    validated=312    rejected=7
dedupe      unique=312
normalize   normalised=312
select      selected=180     trimmed_over_target=132
persist     inserted=180     already_known=0    conflicted=0
split       train=144        val=36
export      180 records      fingerprint=9876e8c83245194b
```

**Dataset composition** — 60 per class, exactly the brief's 40-60 band:

| class | train | val | total | train % |
|---|---|---|---|---|
| bird | 48 | 12 | 60 | 80.0 |
| cat | 48 | 12 | 60 | 80.0 |
| dog | 48 | 12 | 60 | 80.0 |

Stratification is exact — the ratio holds *inside* each class rather than only
in aggregate.

**Sources** — both required collection methods contributed:

| source | method | images |
|---|---|---|
| Openverse | public API | 156 (bird 60, cat 49, dog 47) |
| Wikimedia Commons | HTML scraping | 24 (cat 11, dog 13) |

Selection is source-blind — it ranks on the content hash — so the scraped source
survives the trim in proportion to what it contributed. Commons supplements two
of the three classes; the brief asks for at least one.

**Licences** — every image is usable for commercial model training:

```
CC-BY-2.0    90     CC-BY-SA-2.0   44     PDM-1.0    21
CC-BY-SA-4.0 14     CC0-1.0         7     CC-BY-4.0   3     CC-BY-3.0  1
```

Zero NC and zero ND, by construction — see the licence policy above.

**What was rejected, and why**

| reason | n | what it was |
|---|---|---|
| `select/OVER_TARGET` | 132 | valid images beyond the 60-per-class target — recorded, not silently dropped |
| `validate/UNSUPPORTED_FORMAT` | 7 | vector and non-photographic formats served from Commons category listings |
| `download/TOO_LARGE` | 2 | files above the 20 MB streaming cap |

Every rejection is a queryable row in `rejections`, not a log line — and
`run_summary` deliberately keeps the two kinds apart:

```
 run      | fetched | kept | rejected | trimmed | rejection_rate_pct
----------+---------+------+----------+---------+--------------------
 48f4d178 |     321 |  180 |        9 |     132 |                2.8
 2ce3e5c7 |       0 |    0 |        0 |       0 |
```

A **2.8% defect rate** against live sources. `OVER_TARGET` is a capacity
decision, not a fault, so it is counted separately: folding it into the
rejection rate would put a perfectly healthy run at 44% and destroy the one
number this view exists to make comparable across runs.

**The second run, immediately after.** Run
`2ce3e5c7-86c0-4e16-a213-0efc6be243cc`, exit code 0, **one second**:

```
ingest      fetched=0   classes_at_target=6
persist     inserted=0  already_known=0
split       changed=0   eligible=180
export      180 records  fingerprint=9876e8c83245194b
```

Zero API calls, zero bytes over the network, zero rows written, not one image
moved between train and val — and the identical fingerprint proves the exported
dataset is the same dataset, not merely a similar one. `classes_at_target=6` is
three classes × two sources that were never asked for anything, because nothing
was needed.

**On near-duplicate detection.** The pHash stage ran over all 312 candidates and
found **none** within Hamming distance 5. That is an honest result, not a
demonstration: this particular collection happens to contain no re-encoded or
resized copies. The mechanism is implemented and tested — `tests/test_dedupe.py`
proves it catches recompressed, resized and greyscale copies while not
collapsing distinct photographs — but it had no effect on this dataset, and the
`duplicate_of` column is empty as a result. Worth knowing before opening the
table and wondering.


---

## Testing

```
tests/
├── conftest.py                    deliberately corrupt image fixtures
├── test_validate.py               one fixture per rejection reason
├── test_dedupe.py                 pHash robustness + false-positive guard
├── test_normalize_and_split.py    licence mapping, stratification, leakage
├── test_select.py                 class targets, determinism, orphan markings
├── test_http_client.py            robots.txt, retries, per-host throttling
├── test_sources.py                adapter parsing against canned payloads
└── test_integration.py            the real pipeline, end to end, twice
```

```bash
make test        # 103 tests in Docker — needs no local Python at all
```

The suite runs in its own build stage against the compose Postgres, so nothing
is skipped and nothing has to be installed first. That is deliberate: a test
suite that requires the reviewer to have Python 3.12 and the right wheels is a
test suite the reviewer does not run, and "103 tests pass" then rests on my word
instead of on one command.

**No test touches the network.** Every messy case is constructed locally, so the
suite is deterministic and runs in CI without credentials. Testing against a
live API would test the API, not this code, and would fail on a Sunday for
reasons nobody can reproduce.

`test_integration.py` runs the *real* pipeline — real HTTP client, real
downloads, real Pillow, real perceptual hashing, real Postgres, real Parquet —
against a local HTTP server serving a deliberately messy corpus. Only the source
adapters are substituted, and only to change where the URLs point. It creates
and drops its own throwaway database, so it never touches the pipeline's data,
and it skips itself when no Postgres is reachable (which is why `make test`
brings one up).

`sql/verify_schema.sql` proves the schema's guarantees independently of Python:
12 assertions covering idempotency, every CHECK constraint, cascade behaviour
and the views, ending in `ROLLBACK` so it leaves no trace. Every assertion is a
*delta* against a baseline taken at the start, so it can be run against the live
database after a real run — which is exactly when anyone will reach for it.

Nine defects were found by testing and by running against the real sources,
rather than by reading the code — which is the argument for doing both:

1. **An image was its own near-duplicate on re-run.** The pHash index is seeded
   from the database, which already contains every image about to be
   re-processed — so each matched itself at distance 0 and the DB rejected the
   self-reference, killing the run. Only ever visible on the *second* run.
2. **A near-duplicate escaped marking.** When a higher-resolution copy displaced
   an incumbent, the incumbent was left unmarked. Fixed by processing
   largest-first, so the best copy is always the incumbent.
3. **Schema application could hang forever.** `DROP TRIGGER … CREATE TRIGGER`
   takes an `ACCESS EXCLUSIVE` lock and blocked behind an idle reader, with no
   error and no timeout.
4. **The scraper silently contributed zero images — `robots.txt` handling.**
   The logs said `robots.txt disallows`, and Commons had disallowed nothing.
   `RobotFileParser.read()` fetches with `User-Agent: Python-urllib/3.x`, which
   Wikimedia rejects with **403** — and the standard library converts a 403 on
   robots.txt into `disallow_all = True`, silently, without raising. The
   crawler then politely refused to fetch a single page from a site that had
   never objected. It looked exactly like correct robots-compliance, which is
   why it survived review. Fixed by fetching robots.txt through our own
   session with our real, identifying User-Agent; a robots file we cannot
   *read* now fails open, while a `Disallow` we can read is still obeyed.
   Nine tests in `tests/test_http_client.py` pin both halves.
5. **Broad Commons categories hold no files.** Found only by inspecting
   the exported manifest of the first real run: every one of the 48 images came
   from the API. The cause was not a broken selector — broad Commons categories
   (`Cats`, `Dogs`, `Birds`) are *container* categories built almost entirely
   from subcategories, holding no media of their own. The scraper now descends
   one level, and a source contributing nothing is now logged as an error
   rather than passing unremarked.
6. **The first dataset was not legally usable for training.** 29 of 48 images
   carried NC or ND terms. See below.
7. **"Re-runs download nothing" was not true.** The claim was there from the
   start and the code never supported it: a content hash is only knowable after
   the transfer, so `known_sha256` could *report* that a re-run fetched nothing
   new but never prevent it. Two docstrings in the same module contradicted each
   other about it. Fixed properly — ingestion is now scoped to what each class
   still needs, and a URL already in the store is reused — so the second run
   genuinely makes no requests. The lesson is the general one: a claim in a
   README is a test that has not been written yet.
8. **The thread pool was serialised by its own rate limiter.** `_HostThrottle`
   slept while holding one global lock, so eight download workers behaved
   exactly like one, and a slow image CDN also blocked requests to Commons,
   which shares nothing with it. The lock now only *reserves* a departure slot
   per host; the waiting happens outside it. Pinned by four tests, because this
   is invisible in any functional test — the code was correct, only slow.
9. **The delivered dataset ignored the brief's size.** 401 images against a
   stated 40-60 per class: `overfetch_factor` was applied on the way in and
   nothing brought the survivors back down, so the target was documentation
   rather than behaviour. `pipeline/select.py` now trims each class, and the
   two failure modes that trim introduced — a dataset creeping past the target
   on every re-run, and a near-duplicate left pointing at an image that was
   itself trimmed — are both covered in `tests/test_select.py`.

---

## Reproducibility

Currently guaranteed:

- **No seeds anywhere.** Split assignment is derived from each image's content
  hash, so it is reproducible from the image alone.
- **Content-addressed storage** — the same bytes always land at the same path.
- **`dataset_fingerprint`** in the manifest: sha256 over every member's checksum
  and split. Two runs producing the same fingerprint produced literally the same
  dataset. This is the cheap version of the question DVC answers, and it makes
  the reproducibility claim *checkable* rather than rhetorical.

  It has been checked. Three runs were made: one against a clean database, one
  immediately after it, and a third against a database and image store both
  wiped back to empty — a full re-collection from the live sources, twenty
  minutes later, with a different `run_id`. All three report

  ```
  fingerprint=9876e8c83245194b
  ```

  The third is the one that matters: the same 180 images, in the same classes,
  on the same sides of the train/val boundary, rebuilt from scratch off the
  open internet. Nothing about that is guaranteed by the sources — it is
  guaranteed by content-derived selection and content-derived splitting, and a
  seeded shuffle could not have produced it.
- **`config_snapshot`** and `git_commit` stored per run, so any dataset can be
  traced back to the exact settings and code that produced it.
- **Pinned dependencies** in `requirements.txt`.

To extend to full dataset versioning: `dvc add data/output` with a remote, using
`dataset_fingerprint` as the tag. Roughly an afternoon; deliberately out of
scope here, since the brief said a sentence or two would do.

---

## What I would do with more time

1. **Replace linear pHash search with a BK-tree**, or store hashes as bit
   vectors in `pgvector` with an HNSW index. The current O(n²) comparison is
   correct and fast at 180 images and becomes the bottleneck around 10⁵.
2. **Move image bytes to object storage.** The `storage_path` column already
   makes this a one-line change to the export; content-addressed keys map
   directly onto an S3 prefix.
3. **Alembic migrations** instead of a single idempotent DDL file, once the
   schema needs to change without dropping the volume.
4. **A work queue for downloads** (Celery / RQ) so ingestion scales past one
   process, with the raw layer as the natural hand-off point.
5. **Track rejection rates over time** and alert on drift. The data is already
   in `run_stage_metrics` and `rejections`; it needs a dashboard, not a schema
   change. A source that quietly starts serving WebP should page someone.
6. **Tune the Hamming threshold empirically** against a hand-labelled set of
   near-duplicate pairs, rather than adopting the conventional value and
   spot-checking it.
7. **Embedding-based near-duplicate detection** (CLIP or similar) to catch
   crops and heavy edits that pHash misses — at the cost of a model dependency
   and much slower ingestion.

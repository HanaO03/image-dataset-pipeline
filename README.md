# Image Dataset Ingestion Pipeline

[![CI](https://github.com/HanaO03/image-dataset-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/HanaO03/image-dataset-pipeline/actions/workflows/ci.yml)

Collects images of **cat / dog / bird** from a public API and by scraping a public
web page, validates and deduplicates them, stores the records and full run
metadata in PostgreSQL, and exports an ML-ready dataset to `/data/output`.

```bash
cp .env.example .env
make up          # or: docker compose up --build
```

That is the whole setup. It builds the image, starts Postgres, applies the
schema, runs the pipeline end to end, writes the dataset to `./data/output`, and
the pipeline container exits 0.

**Two commands, one difference worth knowing.** `make up` adds
`--abort-on-container-exit --exit-code-from pipeline`, so the command returns
when the pipeline finishes and its exit code is the pipeline's. Plain
`docker compose up` stays attached afterwards, because Postgres is still running
and `up` returns only when every service has stopped — deliberate, since the
database staying up is how the results can be queried, but it means the command
does not exit on its own. Ctrl-C, or `docker compose down`, ends it.

**Running it a second time inserts nothing and downloads nothing** —
see [Idempotency](#idempotency).

> This README is the overview. Three companion documents hold the detail, so
> that this one stays readable in about ten minutes:
> **[DECISIONS.md](DECISIONS.md)** — why each choice was made, and the licence
> policy in full · **[RESULTS.md](RESULTS.md)** — the delivered run, measured ·
> **[DEFECTS.md](DEFECTS.md)** — every defect found, and which method found it.

---

## Where each requirement lives

The brief, mapped to the code that answers it.

| Requirement | Implemented in | Detail |
|---|---|---|
| **A1** API collection, 40–60 per class | `src/sources/openverse.py` | 60 per class, delivered — [RESULTS.md](RESULTS.md) |
| **A2** Scraping a real HTML page | `src/sources/wikimedia.py` | Commons category → file page → licence; the API is deliberately *not* used |
| **A3** Messy sources handled, not sanded away | `src/pipeline/validate.py`, `rejections` table | [16 failure modes, each with a reason code and a test](#handling-messy-data) |
| **B** Exact deduplication | `sql/schema.sql` — `images.sha256 UNIQUE` | Enforced by the database, not by Python |
| **B** Near-duplicate detection *(bonus)* | `src/pipeline/dedupe.py` | pHash, Hamming ≤ 5 — [the one stretch goal taken](DECISIONS.md#scope-one-stretch-goal-taken-deliberately) |
| **B** File validation | `src/pipeline/validate.py` | [Decode twice; the `verify()` trap](#handling-messy-data) |
| **B** Normalised metadata schema | `src/pipeline/normalize.py`, `src/models.py` | One schema across both sources; licences → SPDX |
| **B** Postgres: records **and** run metadata | `sql/schema.sql` | [5 tables + 2 views](#database-design) |
| **C** Stratified train/val split | `src/pipeline/split.py` | Content-derived, not a seeded shuffle |
| **C** Export to `/data/output` + manifest | `src/pipeline/export.py` | [`dataset.parquet` · `dataset.csv` · `manifest.json`](#output) |
| **C** Reproducibility note | — | [DECISIONS.md](DECISIONS.md#reproducibility), including where the guarantee stops |
| **D** Dockerfile | `Dockerfile` | Multi-stage, non-root, `tini` |
| **D** `docker compose up` end to end | `docker-compose.yml` | The quickstart above; exit codes `0` / `1` / `2` |
| Database scripts | `sql/schema.sql`, `sql/verify_schema.sql` | DDL + [12 assertions against the live schema](#testing) |
| Sample output committed | `data/output/` | Full manifest/CSV/parquet + [24 real images](#output) |

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
  │  5. NORMALIZE   licence → SPDX; NC/ND refused, whatever the source        │
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
│   ├── normalize.py     licence → SPDX; the licence policy, for every source
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

The choices worth defending — why `sha256 UNIQUE` rather than de-duplicating in
Python, why long-format metrics, why near-duplicates are marked rather than
deleted, and why the schema is applied by the app rather than by
`docker-entrypoint-initdb.d` — are set out in
[DECISIONS.md](DECISIONS.md#database-choices-worth-defending).

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
fingerprint — and confirmed against the live sources
([RESULTS.md](RESULTS.md)).

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
| a licence we *can* read that forbids training | NC/ND gate at normalisation, after both sources converge | `LICENSE_NOT_PERMITTED` |
| 1×1 tracking pixels, favicons | dimension and file-size floors | `DIMENSIONS_TOO_SMALL` |
| banners, spritesheets, diagrams | aspect-ratio bound (> 6:1) | `ASPECT_RATIO_EXTREME` |
| decompression bombs | `Image.MAX_IMAGE_PIXELS` | `IMAGE_BOMB` |
| a 200 response with an empty body | byte count after streaming | `EMPTY_RESPONSE` |
| a file larger than the streaming cap | size enforced mid-stream, not from `Content-Length` | `TOO_LARGE` |
| a host that accepts the connection and never answers | connect and read timeouts on every request | `TIMEOUT` |
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

Licences get their own treatment, because the first real run produced a dataset
that was *correct* and simultaneously *unusable* — 29 of 48 images carried NC or
ND terms. The policy, the two layers that enforce it, and why `all-cc` is the
wrong filter: [DECISIONS.md](DECISIONS.md#licence-policy-and-why-all-cc-is-the-wrong-filter).

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

## Results

`make up` against a clean database and an empty image store, 2026-08-18.
Exit code 0, `status=SUCCESS`, 267 seconds.

| class | train | val | total |
|---|---|---|---|
| bird | 48 | 12 | 60 |
| cat | 48 | 12 | 60 |
| dog | 48 | 12 | 60 |

Both required collection methods contributed: Openverse (API) 154 images,
Wikimedia Commons (HTML scraping) 26. Every licence permits commercial training
— zero NC, zero ND. A **2.5% defect rate** against live sources. The second run,
immediately after, took **1.5 seconds**, made zero API calls, wrote zero rows,
and produced an identical `dataset_fingerprint`.

Full figures — per-source breakdown, licence distribution, every rejection and
what it was, the attribution policy, and one honest miss:
**[RESULTS.md](RESULTS.md)**.

---

## Running and inspecting

```bash
make up             # build + run everything (the main entry point)
make smoke          # quick run, 10 images per class
make status         # recent runs + current dataset composition
make psql           # psql shell against the pipeline database
make verify-schema  # 12 assertions against the live schema, then rolls back
make test           # 172 tests in Docker — no local Python needed
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

**Two licences, and they are not the same one.** The code is MIT
([`LICENSE`](LICENSE)). The images are not: each one keeps the licence its
photographer chose, recorded per image in the manifest and credited in
`ATTRIBUTIONS.txt`. [`NOTICE`](NOTICE) says which file to look in for what.

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

## Testing

```
tests/
├── conftest.py                    deliberately corrupt image fixtures
├── test_validate.py               one fixture per rejection reason
├── test_dedupe.py                 pHash robustness + false-positive guard
├── test_normalize_and_split.py    licence mapping, the NC/ND gate, stratification, leakage
├── test_select.py                 class targets, determinism, orphan markings
├── test_download.py               the four rejections this stage owns
├── test_export.py                 tree/manifest agreement, split-change pruning
├── test_http_client.py            robots.txt, per-host throttling, retries and 429
├── test_sources.py                adapter parsing against canned payloads
└── test_integration.py            the real pipeline, end to end, twice
```

```bash
make test        # 172 tests in Docker — needs no local Python at all
```

The suite runs in its own build stage against the compose Postgres, so nothing
is skipped and nothing has to be installed first. A test suite that requires the
reviewer to have Python 3.12 and the right wheels is a test suite the reviewer
does not run, and "172 tests pass" then rests on my word instead of on one
command.

It also runs on every push — [`.github/workflows/ci.yml`](.github/workflows/ci.yml),
the badge at the top of this file. Lint, the full suite against a real Postgres
service, and a build of both Docker stages.

**No test touches the network.** Every messy case is constructed locally, so the
suite is deterministic and runs in CI without credentials. Testing against a
live API would test the API, not this code, and would fail on a Sunday for
reasons nobody can reproduce.

`test_integration.py` runs the *real* pipeline — real HTTP client, real
downloads, real Pillow, real perceptual hashing, real Postgres, real Parquet —
against a local HTTP server serving a deliberately messy corpus. Only the source
adapters are substituted, and only to change where the URLs point.

`sql/verify_schema.sql` proves the schema's guarantees independently of Python:
12 assertions covering idempotency, every CHECK constraint, cascade behaviour
and the views, ending in `ROLLBACK` so it leaves no trace.

**Twenty-two defects were found**, nine by testing and by running against the
real sources, thirteen by reading — my own documentation against my own code,
and adversarial reviews looking for claims the code did not support. Which method
found which is the interesting part, and it is the part a green test suite cannot
tell you. Every one, with its cause, is in **[DEFECTS.md](DEFECTS.md)**.

The five worth reading if you only read five:

- **The scraper obeyed a `robots.txt` that forbade nothing.**
  `RobotFileParser.read()` fetches with `User-Agent: Python-urllib/3.x`, Wikimedia
  answers **403**, and the standard library silently converts a 403 into
  `disallow_all`. The crawler then politely refused to fetch a single page from a
  site that had never objected — and it looked exactly like correct
  robots-compliance, which is why it survived review.
- **The export reintroduced train/val leakage at the last step.** Splitting runs
  over the whole stored dataset, so an image can legitimately move from train to
  val as the dataset grows — and `_copy_images` only ever added. The same
  photograph then existed under both. The manifest is generated from the database
  and stayed correct, which is what hid it: `ImageFolder` walks directories, not
  manifests.
- **The thread pool was serialised by its own rate limiter.** `_HostThrottle` slept
  while holding one global lock, so eight download workers behaved exactly like
  one, and a slow image CDN also blocked requests to Commons, which shares nothing
  with it. Invisible to any functional test — the code was correct, only slow.
- **The licence parser manufactured `CC-BY-4.0` out of ordinary English.** A
  `findall(r"\b(by|nc|nd|sa)\b")` across the raw string turned `All rights
  reserved. Photo by Jane Doe` into a Creative Commons grant, so an
  all-rights-reserved image would have been relabelled as the most permissive
  licence the dataset accepts and shipped as training data.
- **A test reddened CI once every sixteen runs.** It asserted that the second
  backoff delay exceeds the first — comparing two *jittered* draws, whose bands
  overlap by construction, so a correct implementation lost that comparison
  **6.3%** of the time. Measured over 200,000 simulated draws. A test that fails at
  random teaches everyone to re-run the job rather than read it.

---

## Reproducibility

- **No seeds anywhere.** Split assignment is derived from each image's content
  hash, so it is reproducible from the image alone.
- **Content-addressed storage** — the same bytes always land at the same path.
- **`dataset_fingerprint`** in the manifest: sha256 over every member's checksum
  and split. Three runs that had every reason to disagree — a clean
  re-collection, an immediate re-run, and a run from a fresh `git clone` — all
  reported `fingerprint=3c34b24dc3c73f22`.
- **`config_snapshot`** and `git_commit` stored per run, so any dataset can be
  traced back to the exact settings and code that produced it.
- **Pinned dependencies** in `requirements.txt`.

What this *cannot* guarantee is that the sources offer the same candidates on a
different day — and the boundary is the interesting part. It is set out, with
the measurement that establishes it, in
[DECISIONS.md](DECISIONS.md#reproducibility).

---

## Further reading

| document | what is in it |
|---|---|
| [DECISIONS.md](DECISIONS.md) | every design choice and the alternative it beat; the licence policy in full; reproducibility's boundary; what I would do with more time |
| [RESULTS.md](RESULTS.md) | the delivered run, measured: sources, licences, rejections, attribution, one honest miss |
| [DEFECTS.md](DEFECTS.md) | twenty-two defects, each with its cause and the method that found it |

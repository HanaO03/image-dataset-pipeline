# Decisions

The reasoning behind the choices the README states but does not argue. Ordered
roughly by how much a reviewer is likely to disagree with them.

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

### Scope: one stretch goal, taken deliberately

The brief allows at most one, so the choice was made on which one earns its
keep. **Near-duplicate detection via perceptual hashing** was the answer,
because it is the only option on the list that changes whether the *dataset* is
correct rather than how it is packaged: a re-encoded copy landing on the
opposite side of the train/val boundary makes validation accuracy a lie, and no
amount of orchestration or annotation formatting would catch that. It is also
listed as the bonus in Part B, so it counts twice.

The other four were declined on purpose, not skipped:

| not done | why |
|---|---|
| Airflow / Prefect / Dagster | `docker compose up` must run end to end. An orchestrator inside compose is startup risk for no benefit at 180 images — and each stage is already shaped like a task, so adopting one is mechanical. |
| DVC / Git LFS | `dataset_fingerprint` answers the question DVC is wanted for — "is this the same dataset?" — in sixteen characters and no extra tooling. The upgrade path is one command, noted under Reproducibility. |
| Polars / PyArrow | Pandas touches 180 rows once, at export. Swapping the engine would be a change with no measurable effect, made to look like range. |
| COCO-format manifest | COCO describes bounding boxes and segmentation masks. This is a classification dataset with none of either, so a COCO manifest would be mostly empty fields — a worse artefact that merely looks more sophisticated. |

---
---

## Database: choices worth defending


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

## Licence policy, and why `all-cc` is the wrong filter

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
of failure.

**The policy is enforced in two places, and it needs both.**

| layer | what it does | why it is not enough alone |
|---|---|---|
| `license_type=commercial,modification` on the Openverse query | the images are never fetched | it is a parameter on *one* source's API. Commons is scraped: there is no filter to set, and a file page states whatever it states |
| `LicenseSettings.forbidden_elements` at the normalise stage | rejects `NC` and `ND` from any source, with reason code `LICENSE_NOT_PERMITTED` | it costs a download to discover — which is why the API filter stays |

The second layer was added after the first was found to be doing less than the
documentation claimed. The prose said "the pipeline only collects images that
permit commercial use and modification"; the code said that only about
Openverse, and a scraped NC image would have been stored in a dataset described
as commercially usable. Nothing in the delivered dataset was affected — Commons
happened to supply only free licences — which is precisely what makes the gap
the dangerous kind: correct output, unsound reason, and no failing run to point
at it.

The gate runs *after* both sources converge on one SPDX identifier, so it
cannot be bypassed by adding a third source later. `tests/test_normalize_and_split.py`
pins it, including the case that motivated it: a scraped `CC BY-NC-SA 2.0`
image, rejected.

Verify it on any run:

```bash
python -c "import json; print(json.load(open('data/output/manifest.json'))['licenses'])"
```
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

  It has been checked, and the check is worth stating precisely, because the
  guarantee has a boundary and the boundary is the interesting part.

  **What the pipeline guarantees:** given the same candidates, the exported
  dataset is identical — same images, same classes, same sides of the train/val
  line. Selection and splitting are both derived from image content, with no
  seed and no ordering dependency, so nothing about the execution — thread
  scheduling, row order, the machine — can change the result. Confirmed on
  2026-08-18 by three runs that had every reason to disagree and did not: a
  clean re-collection with the database and image store wiped, an immediate
  re-run against the populated database, and a run from a fresh `git clone` on
  the same machine, all reporting

  ```
  fingerprint=3c34b24dc3c73f22
  ```

  A fourth data point, and the more interesting one: this fingerprint is
  unchanged from the run made *before* the licence-parsing fixes landed. That
  is the correct outcome and worth stating, because it is checkable — those
  defects corrupted the licence *metadata* on four rows, not which images were
  selected or where they landed, so the dataset's identity should not have
  moved, and it did not.

  **What it cannot guarantee:** that the sources offer the same candidates on a
  different day. The runs of 2026-08-17 fetched 321 candidates and produced

  ```
  fingerprint=9876e8c83245194b
  ```

  while 2026-08-18 fetched 322 — Openverse had indexed new images overnight —
  and produced a different, equally valid dataset. That is the live web moving,
  not the pipeline wobbling, and no amount of determinism in this repository can
  hold the internet still. What the fingerprint buys is that the difference is
  *visible in one line* instead of being discovered months later by a confused
  colleague: two runs that disagree say immediately that the input changed,
  while `config_snapshot` and `git_commit` on each run say whether the code did.
  Pinning a dataset against upstream drift is precisely the job DVC exists for —
  the upgrade path noted below, and the reason it is an upgrade rather than
  something this replaces.
- **`config_snapshot`** and `git_commit` stored per run, so any dataset can be
  traced back to the exact settings and code that produced it. The commit id is
  resolved on the host and passed in (`make up`), because the built image
  deliberately carries no `.git` directory and could not resolve it itself — and
  it is resolved with `--dirty`, so a dataset produced from uncommitted edits
  says so instead of claiming a commit that never contained the code that ran.
  A bare `docker compose up` leaves the column empty rather than failing;
  `GIT_COMMIT=$(git rev-parse --short HEAD) docker compose up` fills it in.
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
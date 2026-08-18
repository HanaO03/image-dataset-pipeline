# Defect log

Every defect found in this project, what caused it, and which method found it.

This list is here because *which method found which* is the interesting part,
and it is the part a green test suite cannot tell you. Nine of these came from
testing and from running against the real sources. The rest came from reading —
my own documentation against my own code, and then adversarial reviews that went
looking specifically for claims the code did not support. The first nine were
invisible to reading; the last thirteen were invisible to a green test suite.

The five worth reading if you only read five: **4** (robots.txt), **13**
(train/val leakage reintroduced at export), **8** (the rate limiter that
serialised its own thread pool), **20** (the licence parser that manufactured
CC-BY out of ordinary prose), and **22** (a test that reddened CI once every
sixteen runs).

---

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
10. **The licence policy covered one source and claimed to cover both.**
   `license_type=commercial,modification` is a parameter on the Openverse
   query. Commons is scraped, so nothing filtered it: an `NC` or `ND` licence
   on a file page normalised to a valid SPDX identifier and was stored, in a
   dataset the README described as usable for commercial training. No run ever
   failed over it, because Commons happened to supply only free licences — the
   output stayed correct while the reason for it did not. Fixed by gating at
   the normalise stage, where both sources have already converged, with
   `LICENSE_NOT_PERMITTED` as its own reason code. Found by re-reading my own
   documentation against my own code, which is the cheapest review there is and
   the one most easily skipped. Four of the twenty-two reason codes turned out
   to have no test at the same time (`NOT_AN_IMAGE`, `TOO_LARGE`,
   `EMPTY_RESPONSE`, `TIMEOUT`), as did the whole 429 path — all now covered in
   `tests/test_download.py` and `tests/test_http_client.py`. Same lesson as
   defect 7, learned again: a claim in a README is a test that has not been
   written yet.
11. **The site footer was read as the file's licence.** Every MediaWiki page
   ends with "Text is available under CC BY-SA 4.0", linking that deed. It
   describes Commons, not the photograph. The scraper searched the whole
   document for a `creativecommons.org` anchor, so on any page whose own
   licence had no CC deed — GFDL, for instance — the footer answered first and
   the file was stamped `CC-BY-SA-4.0` and shipped as training data under a
   licence its author never granted. `MISSING_LICENSE` was therefore
   unreachable on any real Commons page. The milder version had already shipped
   and is visible in the delivered `dataset.csv`: four rows reading
   `license=CC0-1.0` beside a `by-sa/4.0` URL, because the URL extractor
   preferred `/licenses/` over `/publicdomain/` across the whole page and
   reached past the file's own deed to the footer's. Fixed by stripping page
   chrome before anything is read and confining every licence lookup to the
   file's own licensing container. **The fixtures could not have caught it:**
   hand-written test HTML has no page furniture. They all carry a real footer
   now.
12. **`NoDerivs` was not recognised as ND.** The long-form matcher required the
   literal `derivat`, which only appears in the 4.0 spelling
   "NoDerivatives"; CC 2.0 and 3.0 render "NoDerivs". So
   `Attribution-NoDerivs 3.0` normalised to `CC-BY-3.0` — the single most
   restrictive licence in the set relabelled as one of the most permissive, and
   the NC/ND gate downstream had nothing left to catch. The same design flaw
   dropped ND from `Attribution-NonCommercial-NoDerivs 2.0`, because the
   matcher returned the elements of the first whole-name pattern that matched
   instead of accumulating every element present. Now one pattern per element,
   all of them tested.
13. **The export never pruned, so a re-split leaked.** Splitting runs over the
   whole stored dataset, so an image can legitimately move from train to val as
   the dataset grows — and `_copy_images` only ever added. The previous copy
   stayed exactly where it was, and the same photograph then existed under
   `images/train/cat/` *and* `images/val/cat/`. The manifest is generated from
   the database and stayed correct, which is what hid it; `ImageFolder` does
   not read the manifest, it walks directories. Train/val leakage — the failure
   the entire deduplication stage exists to prevent — reintroduced at the last
   step by the one stage nobody thought of as making decisions.
   `tests/test_export.py` now asserts the tree and the manifest describe the
   same set of files, after a reshuffle.
14. **`docker compose up` does not exit, and the first line of this README said
   it did.** The pipeline container exits 0; Postgres keeps running, and `up`
   returns only when every service stops. `make up` passes
   `--abort-on-container-exit --exit-code-from pipeline` and does return. The
   quickstart now says so — a small thing, on the most-read line in the
   repository.
15. **A rate-limited source failed the entire run.** `RateLimitedError`
   inherited from `RuntimeError`, and every source adapter handles a failed
   fetch with `except requests.RequestException` — which is the correct thing
   to write, and which this exception walked straight past. It reached the
   runner's catch-all and marked the run `failed`. So the most ordinary
   failure a polite scraper meets, the one the retry logic and the per-host
   delays exist to survive, was the one thing that could kill a run outright —
   while the messy-data table two sections up promised `429 → backoff →
   HTTP_ERROR`. Fixed by making the exception a `requests.RequestException`,
   which corrects every call site at once, including ones not yet written.
   `download.py` had it right already, by catching the specific type first.
16. **`.env` was read in Docker and ignored outside it.** `env_file` was set on
   the root `Settings` class, which covers the root's own fields; every nested
   group (`SourceSettings`, `DedupeSettings`, …) is a separate `BaseSettings`
   and read `os.environ` alone. `SOURCE_TARGET_PER_CLASS=7` in `.env` produced
   a run of 60, silently. Under compose it worked — `env_file:` there injects
   the file as real environment variables — so the gap was invisible on the
   documented path and visible only to someone running locally, after a README
   that opens with `cp .env.example .env`.
17. **Two attributions credited the wrong thing entirely.** The author fallback
   selector took the cell after the *first* label in the Commons information
   table, and that label is "Description". One row credited a montage's
   component list, 42 words, truncated mid-word at 500 characters; another
   carried a rendered `{{Creator}}` block complete with VIAF and ISNI numbers.
   Both shipped. The selector now matches the label text, and the value is
   reduced to a credit line rather than a catalogue record.
18. **A claim was checked on part of the data and written about all of it.**
   "No attribution exceeds 200 characters", said of the delivered CSV, with the
   reader explicitly invited to verify it. Twenty rows do; the longest is 351.
   The 200-character cap is real but applies only to the cell scraped from
   Commons — and the check that produced the sentence had filtered to Commons
   rows without the sentence saying so. Openverse attributions are the API's
   own recommended credit line, licence sentence and deed URL included, and
   passing them through unchanged is correct: trimming them would damage the
   credit they exist to preserve. Two sentences now say which source they are
   about, and the command in the README is one that passes.
19. **Three documentation counts had drifted.** The CI workflow still said
   "103 tests", the requirement table said 15 failure modes against a table of
   16, and a config comment pointed at README text that says the opposite of
   what the comment claims. Each is trivial; together they are the thing this
   project keeps having to relearn, which is why they are listed rather than
   quietly corrected.

20. **The licence parser manufactured `CC-BY-4.0` out of ordinary English.**
   The coded-element matcher ran `re.findall(r"\b(by|nc|nd|sa)\b", …)` across
   the whole raw string, so any sentence containing the ordinary word "by"
   produced a Creative Commons licence out of nothing: `"All rights reserved.
   Photo by Jane Doe"` → `CC-BY-4.0`. An all-rights-reserved image was
   therefore not rejected — it was relabelled as the most permissive licence
   the dataset accepts, passed the NC/ND gate because no forbidden element was
   present to catch, and would have shipped as training data under a licence
   nobody granted. A scraped `Author` or `Description` cell reaching that
   function is ordinary prose, so the input was routine rather than
   adversarial. Fixed by anchoring the coded form to the whole string and
   requiring an explicit Creative Commons marker before prose is read as a
   licence. Nothing in the delivered dataset was affected — all 180 rows arrive
   in coded form and normalise identically — which is what made it the
   dangerous kind: correct output, unsound reason, no failing run to point at.
   Same lesson as defects 7 and 10, and the third time it has been the licence
   layer.
21. **A public-domain claim could smuggle a restriction past the NC/ND gate.**
   The public-domain shortcut ran before element parsing, which is right — PD
   identifiers have no elements to parse. What was wrong is that it also ran
   before *noticing a restriction*, and it matched its needles anywhere in the
   string. `"Public domain in the US only; CC BY-NC 4.0 elsewhere"` returned
   `PDM-1.0`; because that identifier does not begin `CC-`, `license_elements`
   returned an empty set, `license_permits` had nothing to intersect, and an
   explicitly NonCommercial image entered the dataset labelled public domain.
   Jurisdiction-qualified prose of exactly this shape is ordinary on Commons
   licence boxes. Now an ambiguous string is rejected rather than resolved in
   either direction. Found alongside 20, in the same re-reading.
22. **A test reddened CI once every sixteen runs.**
   `test_backoff_grows_and_is_jittered` asserted that the second retry delay
   exceeds the first — comparing two *jittered* draws. With 50–150% jitter the
   bands overlap by construction (attempt 1 spans [0.5×, 1.5×] of the nominal,
   attempt 2 spans [1×, 3×]), so a perfectly correct implementation loses that
   comparison **6.3%** of the time. Measured, not estimated: 200,000 simulated
   draws, and one real failure in six full local runs. The badge at the top of
   the README is the first thing anyone sees, and a test that fails at random
   teaches everyone to re-run the job rather than read it — which is worse than
   not having written it. Split into two tests: growth asserted against the
   nominal delay with the jitter pinned, jitter asserted separately over twenty
   draws and against its documented band. This is the only defect on the list
   that was never a bug in the pipeline — the code was always right, and the
   test was wrong about it.

# Results from the delivered run

`make up` against a clean database and an empty image store, 2026-08-18. Run
`79d63002-01eb-4ffe-ac14-81da38ef55be`, exit code 0, `status=SUCCESS`, 267
seconds, `git_commit=4728ccc-dirty`.

> **Why `-dirty`, and why the manifest names a different run.** The commit id is
> resolved with `git describe --dirty`, and the tree was dirty for an honest
> reason: forcing a clean re-collection means deleting the previous
> `data/output`, which is tracked. The label says so rather than naming a commit
> whose tree is not what ran.
>
> The committed `manifest.json` is from `370aa2de-…`, the idempotent re-run
> quoted at the end of this section — same fingerprint, same 180 images, so the
> artefact describes the same dataset the figures below describe. That is the
> reproducibility claim demonstrated rather than asserted.

```
ingest      fetched=322   openverse=270   wikimedia_commons=52   sources_empty=0
download    downloaded=320   failed=2
validate    validated=314    rejected=6
dedupe      unique=314
normalize   normalised=314
select      selected=180     trimmed_over_target=134
persist     inserted=180     already_known=0    conflicted=0
split       train=144        val=36
export      180 records      pruned=0      fingerprint=3c34b24dc3c73f22
```

**Dataset composition** — 60 per class, exactly the brief's 40-60 band:

| class | train | val | total | train % |
|---|---|---|---|---|
| bird | 48 | 12 | 60 | 80.0 |
| cat | 48 | 12 | 60 | 80.0 |
| dog | 48 | 12 | 60 | 80.0 |

Stratification is exact — the ratio holds *inside* each class rather than only
in aggregate. No checksum appears under two splits: the exported tree and the
manifest describe the same 180 files, which `tests/test_export.py` asserts and
which is checkable on the delivered artefacts in one command.

**Sources** — both required collection methods contributed:

| source | method | images |
|---|---|---|
| Openverse | public API | 154 (bird 58, cat 49, dog 47) |
| Wikimedia Commons | HTML scraping | 26 (bird 2, cat 11, dog 13) |

Selection is source-blind — it ranks on the content hash — so the scraped source
survives the trim in proportion to what it contributed. Commons reaches all
three classes here; the brief asks for at least one. Bird contributes only two
because `Category:Birds` is a container category whose files live in
subcategories, and the scraper descends exactly one level.

**Licences** — every image is usable for commercial model training:

```
CC-BY-2.0    88     CC-BY-SA-2.0   44     PDM-1.0      22
CC-BY-SA-4.0 14     CC0-1.0         6     CC-BY-4.0     3
CC-BY-SA-3.0  2     CC-BY-3.0       1
```

Zero NC and zero ND — now by construction rather than by luck, since the gate
that enforces it runs over both sources. Every `license` also agrees with its
`license_url`, which was not true of the previous export: four rows read
`CC0-1.0` beside a `by-sa/4.0` deed URL taken from the Commons page footer. Both
the parsing defect and the check that catches it are described under
[Handling messy data](README.md#handling-messy-data).

**Attribution** — and here the two sources are treated differently on purpose,
because the same word means different things on each.

*Openverse* returns a ready-made credit line: `"Title" by Author is licensed
under CC BY 2.0. To view a copy of this license, visit https://…`. That is the
form Creative Commons recommends, licence sentence and deed URL included, and it
is passed through **unchanged**. Twenty of the 154 run past 200 characters, the
longest at 351 — trimming them would damage the credit they exist to preserve.

*Commons* has no such field. What the scraper reads is a table cell, and a cell
can contain anything the uploader put there, which is why that side is cleaned
and capped at 200 characters. Two rows in the previous export credited a
paragraph of description rather than an author, one cut mid-word at 500
characters. What is checkable on the delivered CSV, and what those defects would
now fail:

```bash
python -c "import csv; rows=[r for r in csv.DictReader(open('data/output/dataset.csv',encoding='utf-8')) if r['source']=='wikimedia_commons']; print(max(len(r['attribution']) for r in rows))"
# 198 — every scraped credit is within the cap, and none carries a VIAF or ISNI identifier
```

**What that number proves, and what it does not.** It proves the cap is
enforced and the authority records are gone: no row exceeds 200 characters, and
none carries a VIAF, ISNI or GND identifier. It does not prove the cell holds a
credit. One of the 26 scraped rows still reads as a `{{Creator}}` biography
rather than a name — `Geoff Charles (1909–2002) Description Welsh photographer
and photojournalist Date of birth/death …` — trimmed to length and stripped of
identifiers, but still a catalogue entry where a credit belongs. Length is a
bound on the damage, not a definition of the field, and presenting a
measurement of the first as evidence about the second is defect 18 committed a
second time. The remaining case is a shape `_clean_author` does not yet
recognise, and it is one row rather than the class being solved.

One scraped credit is a list rather than a name: the montage below has several
photographers, its Commons page says so, and the scraper reports what the page
says. A credit line that names four people because four people are owed credit
is correct; it only looks wrong next to a sentence promising one name each,
which is why that sentence is not made here.

**One honest miss.** `Bird_Diversity_2011.png` is a montage of several species,
not a photograph of one bird, and it passed every check: it is a valid PNG of
reasonable size and ordinary aspect ratio. Nothing cheap distinguishes a
composite from a subject — that is what the embedding-based work under
[what I would do with more time](DECISIONS.md#what-i-would-do-with-more-time) would buy, and
it is worth knowing the dataset contains it. Its attribution is the
multi-photographer list mentioned above, for the same reason: the page is a
montage and its author field is a list. Both are one defect — the image should
not have been collected — and neither is an attribution bug.

**What was rejected, and why**

| reason | n | what it was |
|---|---|---|
| `select/OVER_TARGET` | 134 | valid images beyond the 60-per-class target — recorded, not silently dropped |
| `validate/UNSUPPORTED_FORMAT` | 6 | vector and non-photographic formats served from Commons category listings |
| `download/TOO_LARGE` | 2 | files above the 20 MB streaming cap |

Every rejection is a queryable row in `rejections`, not a log line — and
`run_summary` deliberately keeps the two kinds apart:

```
 run      | fetched | kept | rejected | trimmed | rejection_rate_pct
----------+---------+------+----------+---------+--------------------
 79d63002 |     322 |  180 |        8 |     134 |                2.5
 370aa2de |       0 |    0 |        0 |       0 |
```

A **2.5% defect rate** against live sources. `OVER_TARGET` is a capacity
decision, not a fault, so it is counted separately: folding it into the
rejection rate would put a perfectly healthy run at 44% and destroy the one
number this view exists to make comparable across runs.

**The second run, immediately after.** Run
`370aa2de-6e11-4bc9-a351-004e45b23017`, exit code 0, **1.5 seconds**:

```
ingest      fetched=0   classes_at_target=6
persist     inserted=0  already_known=0
split       changed=0   eligible=180
export      180 records  pruned=0   fingerprint=3c34b24dc3c73f22
```

Zero API calls, zero bytes over the network, zero rows written, not one image
moved between train and val, and nothing pruned from the exported tree — the
identical fingerprint proves the exported dataset is the same dataset, not
merely a similar one. `classes_at_target=6` is three classes x two sources that
were never asked for anything, because nothing was needed.

**On near-duplicate detection.** The pHash stage ran over all 314 candidates and
found **none** within Hamming distance 5. That is an honest result, not a
demonstration: this particular collection happens to contain no re-encoded or
resized copies. The mechanism is implemented and tested — `tests/test_dedupe.py`
proves it catches recompressed, resized and greyscale copies while not
collapsing distinct photographs — but it had no effect on this dataset, and the
`duplicate_of` column is empty as a result. Worth knowing before opening the
table and wondering.


---
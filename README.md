# Project Aftershock — Regional Seismic Risk Triage

A catastrophe-modeling / claims-triage pipeline that ingests a live seismic
event feed, reconciles it against internal sensor/claims records, and flags
which events warrant an immediate loss estimate versus routine review.

## 1. Objectives

Build an automated first-pass triage flag for recent seismic events: given a
live pull from the USGS earthquake catalog, determine which events warrant an
immediate regional loss/impact estimate versus which can wait for routine
review. The output is a single binary `significant` column that a claims
adjuster or catastrophe modeler can act on directly.

## 2. Resource Audit

| Resource | Detail |
|---|---|
| API access | None needed. USGS Earthquake Catalog is fully public domain. |
| Rate limit | Not strictly published — one well-scoped query per run, no polling loop. |
| Data sources | USGS FDSN Event query endpoint (1 call), `data/raw/regional_sensor_log.csv` (generated, never downloaded), 1 web scrape (Alaska Earthquake Center magnitude-classes page). |
| Estimated time | 3–5 hours. |

**A note on this specific run:** the sandbox this pipeline was assembled in
blocks outbound requests to `earthquake.usgs.gov` and `earthquake.alaska.edu`
at the network layer, so `src/pipeline.py`'s live `requests.get()` calls fail
here with a `RequestException` and the code falls back to a locally cached
response (`data/raw/usgs_sample_cache.json`, built from a real, live USGS pull
fetched separately) so the pipeline can still be validated end-to-end. In any
normal environment with outbound internet access, `pipeline.py` hits both
live endpoints directly — no code change needed.

## 3. Target Definition

```
significant = 1 if magnitude >= 5.0
significant = 0 otherwise
```

5.0 is the seismological "moderate" classification floor (minor / light /
moderate / strong / major / great). This is deliberately *not* the rarer
"great" (8.0+) floor: at `minmagnitude=2.5`, magnitude-8+ events are so rare
that a typical two-week pull could easily contain zero of them, which would
make the target column unusable for downstream validation. Magnitude-5+
events are still a minority of any pull, but a workable one.

## 4. Features (brainstormed, minimum 6)

1. `mag` — magnitude
2. `depth_km` — event depth
3. `sig` — USGS's own composite significance score
4. `felt` — felt-report count
5. `gap` — station azimuthal gap (data-quality proxy)
6. `tsunami` — tsunami flag
7. `type` — event type (earthquake / explosion / quarry blast)
8. `region` — parsed from `place`

## 5. Imputation Rationale (semantic vs. statistical)

- `felt`, `cdi` → imputed with **0**. Null plausibly means "no felt reports
  were logged" — a semantic zero, not missing data.
- `gap`, `dmin`, `nst` → imputed with the **cohort median**. Null here means
  "unknown station quality," a materially different situation than zero, so
  a statistical fill (computed once, up front, over only the non-null
  values) is used instead.

## 6. ROI Framework

```
pct_workload_reduction = (1 - (n_flagged / n_total)) * 100
```

Framed as claims-triage volume: if only `significant == 1` events
auto-generate a loss-estimate ticket, manual adjuster review volume drops by
`pct_workload_reduction`%. Both `n_total` and `n_flagged` are computed from
the pipeline's own cleaned dataset — not estimated.

**From this run's sample data:** `n_total=42`, `n_flagged=1`, giving a
**97.6% workload reduction** — i.e. only 1 of the 42 earthquake-type events
in this window crossed the magnitude-5.0 significance floor, so 97.6% of
events would be routed to routine review instead of an immediate adjuster
loss estimate.

## 7. Validation Check — Interpretation

Because `sig` is a continuous score rather than a boolean, a 2×2 crosstab
doesn't apply. Instead, the pipeline compares the average `sig` for
`significant == 1` records against `significant == 0` records. In this run,
the single `significant == 1` event had `sig = 385`, versus an average
`sig ≈ 70` across the `significant == 0` cohort. The flag is doing its job:
events crossing the magnitude-5.0 floor carry a dramatically higher USGS
significance score than routine events, which is exactly the separation a
triage flag needs to be useful — a claims adjuster acting only on
`significant == 1` tickets would be prioritizing the events USGS's own
independent scoring also considers most consequential, not an arbitrary
subset.

## 8. Bonus Web Scrape

`great_threshold` is scraped from the Alaska Earthquake Center's
magnitude-classes page, which states the "great" class begins at magnitudes
greater than 8.0. This feeds `pct_of_great_threshold = (mag / great_threshold)
* 100` in Phase 3 — "how close is this event to a 'great' earthquake, as a
percentage." If the scrape fails, the pipeline falls back to the known
value (8.0) rather than crashing.

## Repository Structure

```
project_repo/
├── data/
│   ├── raw/            # extracted_ids.txt, regional_sensor_log.csv (generated),
│   │                   # usgs_sample_cache.json (offline fallback, see note above)
│   └── processed/      # clean_data.csv — pipeline output
├── notebooks/
│   └── exploration.ipynb
├── src/
│   └── pipeline.py     # importable AND runnable end-to-end
├── generate_aftershock_log.py
└── README.md
```

## Running It

```bash
pip install requests

# 1. (First run only) extract ids from a raw USGS pull, then generate the log:
python generate_aftershock_log.py --input-ids data/raw/extracted_ids.txt

# 2. Run the full pipeline:
python src/pipeline.py
```

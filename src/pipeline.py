"""
src/pipeline.py

Project Aftershock -- Regional Seismic Risk Triage.

End-to-end pipeline: pull the live USGS earthquake catalog, reconcile it
against a generated regional sensor log, and produce a cleaned,
feature-engineered, scaled dataset flagging which events warrant an
immediate loss estimate (significant == 1) versus routine review.

Deliberately uses only requests, json, csv, pathlib, and native Python
loops/dicts/comprehensions -- no pandas or numpy, per the brief.

Runnable standalone:
    python src/pipeline.py
or importable:
    from src.pipeline import run_pipeline
"""

import csv
import json
from pathlib import Path

import requests

USGS_ENDPOINT = "https://earthquake.usgs.gov/fdsnws/event/1/query"
ALASKA_MAGNITUDE_PAGE = "https://earthquake.alaska.edu/earthquake-magnitude-classes"

DEFAULT_PARAMS = {
    "format": "geojson",
    "starttime": "2026-08-15",
    "endtime": "2026-08-29",
    "minmagnitude": 2.5,
}

# Fields that are genuinely sparse in the USGS feed (per the brief's
# "Signature Messiness Warning") and need explicit null-handling downstream.
QUALITY_FLAG_FIELDS = ["felt", "cdi", "mmi", "alert", "nst", "dmin", "gap"]

# Imputed with 0: null plausibly means "no felt reports" -- a semantic zero.
SEMANTIC_ZERO_FIELDS = ["felt", "cdi"]

# Imputed with the cohort median: null means "unknown station quality",
# not zero -- a materially different situation.
MEDIAN_IMPUTE_FIELDS = ["gap", "dmin", "nst"]


# ---------------------------------------------------------------------------
# Phase 1 support: fetch the live catalog
# ---------------------------------------------------------------------------

def fetch_usgs_catalog(params=None, cache_path=None):
    """
    Pull the USGS FDSN event catalog for the configured date window.

    Wraps the request in try/except so a network hiccup doesn't crash the
    whole pipeline -- per the brief's type-safety requirement.

    Parameters
    ----------
    params : dict, optional
        Query params to send to the USGS endpoint. Defaults to
        DEFAULT_PARAMS (geojson, 2026-08-15 to 2026-08-29, minmag 2.5).
    cache_path : str or None
        If the live request fails AND cache_path is given, fall back to a
        locally cached geojson file (used for offline/sandboxed testing).

    Returns
    -------
    list[dict]
        The list of raw GeoJSON feature records under payload["features"].
        Returns an empty list if both the live call and the fallback fail.
    """
    params = params or DEFAULT_PARAMS
    try:
        response = requests.get(USGS_ENDPOINT, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
        return payload.get("features", [])
    except requests.exceptions.RequestException as exc:
        print(f"Live USGS request failed ({exc}).")
        if cache_path and Path(cache_path).exists():
            print(f"Falling back to cached response at {cache_path}.")
            with open(cache_path, "r", encoding="utf8") as f:
                payload = json.load(f)
            return payload.get("features", [])
        print("No cache available -- returning an empty feature list.")
        return []


def scrape_great_threshold(url=ALASKA_MAGNITUDE_PAGE, default=8.0):
    """
    Fetch the Alaska Earthquake Center magnitude-classes page and pull the
    numeric floor for the "great" earthquake class out of the sentence
    "...ends with 'great' for magnitudes greater than 8.0...".

    Falls back to `default` if the page or the anchor phrase can't be
    fetched/found, so a scrape hiccup never crashes the pipeline.

    Returns
    -------
    float
    """
    anchor = "magnitudes greater than"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        text = response.text
        idx = text.lower().find(anchor)
        if idx == -1:
            print(f"Anchor phrase not found on {url}; using default {default}.")
            return default

        # Print the raw window so we can see its exact shape before trusting it.
        window = text[idx: idx + 60]
        print(f"Scrape window: {window!r}")

        after_anchor = text[idx + len(anchor):].strip()
        # The number comes right after the anchor; isolate it by splitting
        # on the first comma, space, or period.
        token = after_anchor
        for sep in [",", " ", "."]:
            if sep in token:
                token = token.split(sep)[0]
                break
        # Handle a leading period from "than 8.0." style punctuation.
        token = token.strip(".")
        return float(token)
    except (requests.exceptions.RequestException, ValueError) as exc:
        print(f"Scrape failed ({exc}); using default great_threshold={default}.")
        return default


# ---------------------------------------------------------------------------
# Phase 2 support: structural audit (used interactively in the notebook,
# reproduced here so pipeline.py is fully self-contained/runnable)
# ---------------------------------------------------------------------------

def walk_record(record, path="root"):
    """
    Recursively walk a nested dict/list record, printing the type of every
    leaf value alongside its dotted path. Works for any nested API payload,
    not just this one: dicts recurse into every value, lists recurse into
    their first item, anything else is a leaf.
    """
    if isinstance(record, dict):
        for key, value in record.items():
            walk_record(value, path=f"{path}.{key}")
    elif isinstance(record, list):
        if record:
            walk_record(record[0], path=f"{path}[0]")
        else:
            print(f"{path}: <empty list>")
    else:
        print(f"{path}: {type(record).__name__} = {record!r}")


# ---------------------------------------------------------------------------
# Phase 3 support: cleaning, imputation, feature engineering
# ---------------------------------------------------------------------------

def _running_min_max_mean(values):
    """
    Single-pass min/max/mean over a list of numbers using running
    accumulators -- no min()/max()/sum() builtins, per the brief.
    """
    running_min = None
    running_max = None
    running_sum = 0.0
    count = 0
    for v in values:
        if v is None:
            continue
        if running_min is None or v < running_min:
            running_min = v
        if running_max is None or v > running_max:
            running_max = v
        running_sum += v
        count += 1
    mean = running_sum / count if count else None
    return running_min, running_max, mean


def _median(values):
    """Median over the non-null values in a list, via a single sort."""
    clean = sorted(v for v in values if v is not None)
    n = len(clean)
    if n == 0:
        return None
    mid = n // 2
    if n % 2 == 1:
        return clean[mid]
    return (clean[mid - 1] + clean[mid]) / 2


def null_rate_report(records, fields):
    """
    For each field in `fields`, count how many records have a None value
    and express it as a percentage of the total record count.

    Returns
    -------
    dict[str, float]  field -> percent null
    """
    total = len(records)
    report = {}
    for field in fields:
        null_count = 0
        for r in records:
            if r["properties"].get(field) is None:
                null_count += 1
        report[field] = (null_count / total * 100) if total else 0.0
    return report


def type_breakdown(records):
    """
    Count how many records fall under each distinct `type` value, using a
    running dict built with .get(key, 0) + 1 -- this is where the
    explosion/quarry-blast contamination becomes visible.
    """
    counts = {}
    for r in records:
        t = r["properties"].get("type")
        counts[t] = counts.get(t, 0) + 1
    return counts


def _split_place(place):
    """
    Split a USGS `place` string like "18 km NE of Amboy, Washington" into
    (distance_direction_prefix, location). Some offshore events don't
    contain " of " at all -- in that case the whole string is the location
    and the prefix is None.
    """
    if place is None:
        return None, None
    if " of " in place:
        prefix, location = place.split(" of ", 1)
        return prefix.strip(), location.strip()
    return None, place.strip()


def _depth_category(depth_km):
    """Bucket a depth in km into shallow / intermediate / deep bands."""
    if depth_km is None:
        return None
    if depth_km < 70:
        return "shallow"
    if depth_km <= 300:
        return "intermediate"
    return "deep"


def clean_and_engineer(raw_features, great_threshold):
    """
    Phase 3: cohort filter, imputation, feature engineering.

    Steps (in one pass over the raw features, per the brief's hint that
    all three engineered columns fit in the same loop):
      1. Drop any record where type != "earthquake".
      2. Split `place` defensively (handles missing " of ").
      3. Compute cohort medians for gap/dmin/nst up front (needs its own
         pass since the median requires the full non-null distribution
         before any single record can be imputed).
      4. For each surviving record: impute felt/cdi with 0, impute
         gap/dmin/nst with the cohort median, derive depth_category and
         pct_of_great_threshold, and compute significant.

    Returns
    -------
    list[dict]  cleaned, feature-engineered records (flat dicts, one per
                surviving event)
    """
    # Step 1: cohort filter -- only true earthquakes belong in this analysis.
    earthquakes = [f for f in raw_features if f["properties"].get("type") == "earthquake"]

    # Step 3 (medians computed up front, over only the earthquake cohort)
    medians = {}
    for field in MEDIAN_IMPUTE_FIELDS:
        values = [f["properties"].get(field) for f in earthquakes]
        medians[field] = _median(values)

    cleaned = []
    for f in earthquakes:
        props = f["properties"]
        geometry = f.get("geometry", {})
        coords = geometry.get("coordinates", [None, None, None])
        lon, lat, depth_km = (coords + [None, None, None])[:3]

        prefix, location = _split_place(props.get("place"))

        mag = props.get("mag")

        record = {
            "event_id": f.get("id"),
            "mag": mag,
            "depth_km": depth_km,
            "sig": props.get("sig"),
            "felt": props.get("felt") if props.get("felt") is not None else 0,
            "cdi": props.get("cdi") if props.get("cdi") is not None else 0,
            "gap": props.get("gap") if props.get("gap") is not None else medians["gap"],
            "dmin": props.get("dmin") if props.get("dmin") is not None else medians["dmin"],
            "nst": props.get("nst") if props.get("nst") is not None else medians["nst"],
            "tsunami": props.get("tsunami"),
            "type": props.get("type"),
            "distance_direction": prefix,
            "region": location,
            "depth_category": _depth_category(depth_km),
            "pct_of_great_threshold": (mag / great_threshold * 100) if mag is not None else None,
            "significant": 1 if (mag is not None and mag >= 5.0) else 0,
        }
        cleaned.append(record)

    return cleaned


def join_sensor_log(records, log_path):
    """
    Phase 3: load the generated regional sensor log into a dict keyed by
    event_id, then attach whatever log fields exist to each cleaned record.
    Uses .get() so records with no matching log row (the ~10% dropped ids)
    simply get None for the log fields -- no crash.
    """
    log_path = Path(log_path)
    if not log_path.exists():
        print(f"Sensor log not found at {log_path}; skipping join (all log fields will be None).")
        log_fields = ["sensor_status", "battery_pct", "last_ping_offset_sec"]
        for r in records:
            for field in log_fields:
                r[field] = None
        return records

    with open(log_path, newline="", encoding="utf8") as f:
        reader = csv.DictReader(f)
        log_by_id = {row["event_id"]: row for row in reader}

    log_fields = ["sensor_status", "battery_pct", "last_ping_offset_sec"]
    for r in records:
        log_row = log_by_id.get(r["event_id"], {})
        for field in log_fields:
            r[field] = log_row.get(field)

    return records


def min_max_scale(records, field, scaled_field_name=None):
    """
    Scale `field` across all records using scaled_x = (x - min) / (max - min).
    Adds a new key (scaled_field_name, default f"{field}_scaled") to every
    record in place.
    """
    scaled_field_name = scaled_field_name or f"{field}_scaled"
    values = [r[field] for r in records if r[field] is not None]
    field_min, field_max, _ = _running_min_max_mean(values)

    span = (field_max - field_min) if (field_min is not None and field_max is not None) else None
    for r in records:
        x = r[field]
        if x is None or not span:
            r[scaled_field_name] = None
        else:
            r[scaled_field_name] = (x - field_min) / span

    return records


def validate_significance_flag(records):
    """
    Compare the average `sig` score for significant == 1 records against
    significant == 0 records. Returns (avg_sig_significant, avg_sig_routine).
    """
    sig_yes = [r["sig"] for r in records if r["significant"] == 1 and r["sig"] is not None]
    sig_no = [r["sig"] for r in records if r["significant"] == 0 and r["sig"] is not None]

    avg_yes = sum(sig_yes) / len(sig_yes) if sig_yes else None
    avg_no = sum(sig_no) / len(sig_no) if sig_no else None
    return avg_yes, avg_no


def write_clean_csv(records, output_path):
    """Write the final cleaned/engineered/joined/scaled records to CSV."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not records:
        print("No records to write.")
        return

    fieldnames = list(records[0].keys())
    with open(output_path, "w", newline="", encoding="utf8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in records:
            writer.writerow(r)

    print(f"Wrote {len(records)} cleaned records to {output_path}.")


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_pipeline(
    params=None,
    cache_path="data/raw/usgs_sample_cache.json",
    log_path="data/raw/regional_sensor_log.csv",
    output_path="data/processed/clean_data.csv",
):
    """
    Run the full Project Aftershock pipeline end to end and return the
    final list of cleaned records (also written to output_path as CSV).
    """
    raw_features = fetch_usgs_catalog(params=params, cache_path=cache_path)
    print(f"Fetched {len(raw_features)} raw features from the USGS catalog.")

    if raw_features:
        print("\n--- Structural audit of a sample record ---")
        walk_record(raw_features[0])

        print("\n--- Native-loop EDA ---")
        mags = [f["properties"].get("mag") for f in raw_features]
        depths = [f["geometry"]["coordinates"][2] if f.get("geometry") else None for f in raw_features]
        mag_min, mag_max, mag_mean = _running_min_max_mean(mags)
        depth_min, depth_max, depth_mean = _running_min_max_mean(depths)
        print(f"mag       -> min={mag_min}, max={mag_max}, mean={mag_mean:.3f}" if mag_mean is not None else "mag -> no data")
        print(f"depth_km  -> min={depth_min}, max={depth_max}, mean={depth_mean:.3f}" if depth_mean is not None else "depth_km -> no data")

        print("\n--- Null-rate audit ---")
        for field, pct in null_rate_report(raw_features, QUALITY_FLAG_FIELDS).items():
            print(f"{field}: {pct:.1f}% null")

        print("\n--- Event-type breakdown ---")
        for t, count in type_breakdown(raw_features).items():
            print(f"{t}: {count}")

    great_threshold = scrape_great_threshold()
    print(f"\ngreat_threshold = {great_threshold}")

    cleaned = clean_and_engineer(raw_features, great_threshold)
    print(f"\n{len(cleaned)} records survived the earthquake-only cohort filter.")

    cleaned = join_sensor_log(cleaned, log_path)
    cleaned = min_max_scale(cleaned, "mag", scaled_field_name="mag_scaled")

    avg_yes, avg_no = validate_significance_flag(cleaned)
    print(f"\nValidation check -- avg sig (significant==1): {avg_yes}")
    print(f"Validation check -- avg sig (significant==0): {avg_no}")

    n_total = len(cleaned)
    n_flagged = sum(1 for r in cleaned if r["significant"] == 1)
    pct_workload_reduction = (1 - (n_flagged / n_total)) * 100 if n_total else 0.0
    print(f"\nn_total={n_total}, n_flagged={n_flagged}, pct_workload_reduction={pct_workload_reduction:.1f}%")

    write_clean_csv(cleaned, output_path)
    return cleaned


if __name__ == "__main__":
    run_pipeline()

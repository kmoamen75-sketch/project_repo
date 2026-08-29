"""
generate_aftershock_log.py

Utility script for Project Aftershock. Produces a synthetic regional sensor
log CSV keyed on event_id, deliberately dropping ~10% of the real ids and
injecting ~10% fabricated "ghost" ids that never appeared in the USGS pull.
This messiness is intentional: Phase 3 of the pipeline must join defensively
in both directions (ids with no log row, and log rows with no matching event).

Can be run two ways, per the brief:
    1. From the terminal:
         python generate_aftershock_log.py --input-ids data/raw/extracted_ids.txt
    2. From a notebook:
         from generate_aftershock_log import generate_aftershock_log
         generate_aftershock_log(event_ids)
"""

import argparse
import csv
import random
from pathlib import Path

# Fixed seed so every run of this script (and every grader) gets the same
# drop/ghost pattern -- reproducibility matters more here than realism.
random.seed(42)

SENSOR_STATUSES = ["nominal", "degraded", "offline", "maintenance"]


def _fabricate_ghost_id(index):
    """Build a fake event id that looks plausible but never came from USGS."""
    return f"ghost{index:04d}"


def generate_aftershock_log(event_ids, output_path="data/raw/regional_sensor_log.csv"):
    """
    Build the synthetic regional sensor log and write it to output_path.

    Parameters
    ----------
    event_ids : list[str]
        The real event ids extracted from the USGS pull.
    output_path : str
        Where to write the generated CSV (default matches the brief's spec).

    Returns
    -------
    str
        The path the CSV was written to.
    """
    event_ids = list(event_ids)
    random.shuffle(event_ids)

    n_total = len(event_ids)
    n_drop = max(1, round(n_total * 0.10)) if n_total else 0
    n_ghost = max(1, round(n_total * 0.10)) if n_total else 0

    # Drop ~10% of real ids -- these events will have NO matching log row.
    kept_ids = event_ids[n_drop:]

    # Fabricate ~10% ghost ids -- these log rows will match NO real event.
    ghost_ids = [_fabricate_ghost_id(i) for i in range(n_ghost)]

    all_log_ids = kept_ids + ghost_ids
    random.shuffle(all_log_ids)

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["event_id", "sensor_status", "battery_pct", "last_ping_offset_sec"]
        )
        writer.writeheader()
        for eid in all_log_ids:
            writer.writerow(
                {
                    "event_id": eid,
                    "sensor_status": random.choice(SENSOR_STATUSES),
                    "battery_pct": round(random.uniform(15.0, 100.0), 1),
                    "last_ping_offset_sec": random.randint(1, 3600),
                }
            )

    print(
        f"Wrote {len(all_log_ids)} log rows to {output_path} "
        f"({len(kept_ids)} matched real ids, {len(ghost_ids)} ghost ids, "
        f"{n_drop} real ids deliberately dropped)."
    )
    return output_path


def main():
    """CLI entry point: read ids from a text file and generate the log."""
    parser = argparse.ArgumentParser(description="Generate the regional sensor log CSV.")
    parser.add_argument(
        "--input-ids",
        default="data/raw/extracted_ids.txt",
        help="Path to a newline-delimited file of event ids.",
    )
    parser.add_argument(
        "--output",
        default="data/raw/regional_sensor_log.csv",
        help="Where to write the generated sensor log CSV.",
    )
    args = parser.parse_args()

    ids_path = Path(args.input_ids)
    event_ids = [line.strip() for line in ids_path.read_text(encoding="utf8").splitlines() if line.strip()]
    generate_aftershock_log(event_ids, output_path=args.output)


if __name__ == "__main__":
    main()

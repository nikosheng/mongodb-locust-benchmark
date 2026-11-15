"""Summarize business throughput and compare repeated runs without mixing event types."""
import argparse
import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path


def summarize(run):
    seconds = run["measurement_seconds"]
    # Incremental emits component MONGO events plus a BUSINESS E2E event.
    scenario = run["config"]["scenario"]
    if scenario == "incremental":
        selected = [op for op in run["operations"] if op["type"] == "BUSINESS" and op["name"] == "adt.refresh_patron_e2e"]
    elif scenario == "ingest":
        selected = [op for op in run["operations"] if op["name"] == "gaming.upsert_patron_day"]
    else:
        selected = [op for op in run["operations"] if op["name"].startswith("profile.")]
    requests = sum(op["requests"] for op in selected)
    failures = sum(op["failures"] for op in selected)
    cfg = run["config"]
    slo = cfg.get("slo", {})
    # A per-operation worst p99 is intentional; averaging percentiles would be misleading.
    worst_p99 = max((op["p99_ms"] or 0 for op in selected), default=0)
    arrivals = run["arrivals"]
    arrival_ok = (not arrivals["dropped"] and arrivals["started"] >= arrivals["scheduled"]
                  and arrivals["completed"] + arrivals["failed"] >= arrivals["started"])
    error_rate = failures / requests if requests else 0
    passed = bool(run["valid"] and (requests > 0 or run.get("batch")) and
                  error_rate <= slo.get("max_error_rate", 0.001) and
                  worst_p99 <= slo.get("p99_ms", float("inf")) and
                  (cfg["load"]["mode"] != "arrival" or arrival_ok))
    if run.get("batch") and slo.get("batch_seconds") is not None:
        passed = passed and run.get("batch_seconds") is not None and run["batch_seconds"] <= slo["batch_seconds"]
    return {"run_id": run["run_id"], "label": run["label"], "scenario": scenario,
            "shards": cfg["shards"], "valid": run["valid"], "slo_pass": passed,
            "successful_ops_s": (requests - failures) / seconds if seconds else 0,
            "worst_operation_p99_ms": worst_p99, "error_rate": error_rate,
            "batch_seconds": run.get("batch_seconds"), "arrival_dropped": arrivals["dropped"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("files", nargs="+", help="run-*.json files (shell glob accepted)")
    parser.add_argument("--out", default="result/summary.csv")
    args = parser.parse_args()
    rows = [summarize(json.loads(Path(path).read_text())) for path in args.files]
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    groups = defaultdict(list)
    for row in rows:
        groups[(row["label"], row["scenario"])].append(row)
    for (label, scenario), group in groups.items():
        valid = [r for r in group if r["valid"]]
        rates = [r["successful_ops_s"] for r in valid]
        batches = [r["batch_seconds"] for r in valid if r["batch_seconds"] is not None]
        print(json.dumps({"label": label, "scenario": scenario, "runs": len(group),
                          "valid_runs": len(valid), "slo_pass_runs": sum(r["slo_pass"] for r in group),
                          "median_successful_ops_s": statistics.median(rates) if rates else None,
                          "min_successful_ops_s": min(rates) if rates else None,
                          "max_successful_ops_s": max(rates) if rates else None,
                          "median_batch_seconds": statistics.median(batches) if batches else None}))
    print(f"Saved {out}")


if __name__ == "__main__":
    main()

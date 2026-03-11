#!/usr/bin/env python3
"""
Compute energy summary for Category A benchmarks.

Aggregates all metrics_summary CSVs from a results directory, computing
GPU and CPU energy per query using the same methodology as the metrics
scripts:

    E_gpu = P_steady × t_query
    E_cpu = P_cpu_pkg × t_query

Where:
    P_steady = avg_power_w from steady-state samples (gpu_util >= avg_util)
    t_query  = min query latency (min_ms for Maximus, min_s for Sirius)

This script reads the pre-computed energy values from metrics_summary CSVs,
and also re-derives them for verification. It then produces a unified summary
CSV and a per-benchmark/engine breakdown printed to stdout.

Usage:
    python compute_energy_summary.py                         # default: results/
    python compute_energy_summary.py --results-dir results/  # explicit
    python compute_energy_summary.py --latest                # only latest run per combo
"""
from __future__ import annotations

import argparse
import csv
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
MAXIMUS_DIR = SCRIPT_DIR.parent.parent


def find_metrics_summaries(results_dir: Path) -> list[Path]:
    """Find all *_metrics_summary_*.csv files in the results directory."""
    files = sorted(results_dir.glob("*_metrics_summary_*.csv"))
    return files


def parse_engine(filename: str) -> str:
    """Determine engine from filename: maximus or sirius."""
    if filename.startswith("maximus"):
        return "maximus"
    elif filename.startswith("sirius"):
        return "sirius"
    return "unknown"


def parse_timestamp(filename: str) -> str:
    """Extract timestamp (YYYYMMDD_HHMMSS) from filename."""
    m = re.search(r"_(\d{8}_\d{6})\.csv$", filename)
    return m.group(1) if m else ""


def parse_benchmark_sf(filename: str) -> tuple[str, str]:
    """Extract benchmark and scale factor from filename.

    Examples:
        maximus_tpch_sf1_metrics_summary_20260311.csv -> (tpch, 1)
        maximus_microbench_tpch_sf1_metrics_summary_... -> (microbench_tpch, 1)
        sirius_h2o_sf2gb_metrics_summary_... -> (h2o, 2gb)
    """
    name = filename.replace("_metrics_summary_", "_SPLIT_")
    prefix = name.split("_SPLIT_")[0]  # e.g. maximus_tpch_sf1

    # Remove engine prefix
    for eng in ("maximus_", "sirius_"):
        if prefix.startswith(eng):
            prefix = prefix[len(eng):]
            break

    # Extract SF (last part starting with sf)
    m = re.match(r"(.+)_sf(.+)$", prefix)
    if m:
        return m.group(1), m.group(2)
    return prefix, ""


def load_maximus_summary(path: Path) -> list[dict]:
    """Load a Maximus metrics_summary CSV and normalize to common schema."""
    rows = []
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Use query_time_ms (high precision from elapsed/n_reps) when available,
            # fall back to min_ms (integer, lossy for sub-ms queries)
            query_time_ms = float(row.get("query_time_ms", 0) or 0)
            min_ms = float(row.get("min_ms", 0) or 0)
            if query_time_ms > 0:
                min_s = query_time_ms / 1000.0
            else:
                min_s = min_ms / 1000.0
            avg_power = float(row.get("avg_power_w", 0) or 0)
            avg_cpu_pkg = float(row.get("avg_cpu_pkg_w", 0) or 0)

            # Pre-computed energy from metrics script
            energy_j = float(row.get("energy_j", 0) or 0)
            cpu_energy_j = float(row.get("cpu_energy_j", 0) or 0)

            # Re-derive for verification
            derived_gpu_e = avg_power * min_s
            derived_cpu_e = avg_cpu_pkg * min_s

            rows.append({
                "engine": "maximus",
                "benchmark": row.get("benchmark", ""),
                "sf": row.get("sf", ""),
                "query": row.get("query", ""),
                "storage": row.get("storage", "gpu"),
                "n_reps": int(row.get("n_reps", 0) or 0),
                "min_s": round(min_s, 6),
                "avg_power_w": round(avg_power, 2),
                "avg_gpu_util": float(row.get("avg_gpu_util", 0) or 0),
                "max_mem_mb": float(row.get("max_mem_mb", 0) or 0),
                "num_steady_samples": int(row.get("num_steady_samples", 0) or 0),
                "gpu_energy_j": round(energy_j, 4),
                "gpu_energy_derived_j": round(derived_gpu_e, 4),
                "avg_cpu_pkg_w": round(avg_cpu_pkg, 2),
                "cpu_energy_j": round(cpu_energy_j, 4),
                "cpu_energy_derived_j": round(derived_cpu_e, 4),
                "total_energy_j": round(energy_j + cpu_energy_j, 4),
                "status": row.get("status", ""),
                "source_file": path.name,
            })
    return rows


def load_sirius_summary(path: Path) -> list[dict]:
    """Load a Sirius metrics_summary CSV and normalize to common schema."""
    rows = []
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            min_s = float(row.get("min_s", 0) or 0)
            avg_power = float(row.get("avg_power_w", 0) or 0)
            avg_cpu_pkg = float(row.get("avg_cpu_pkg_w", 0) or 0)

            # Pre-computed energy from metrics script
            gpu_energy_j = float(row.get("gpu_energy_j", 0) or 0)
            cpu_energy_j = float(row.get("cpu_energy_j", 0) or 0)

            # Re-derive for verification
            derived_gpu_e = avg_power * min_s
            derived_cpu_e = avg_cpu_pkg * min_s

            rows.append({
                "engine": "sirius",
                "benchmark": row.get("benchmark", ""),
                "sf": row.get("sf", ""),
                "query": row.get("query", ""),
                "storage": "gpu",
                "n_reps": int(row.get("n_reps", 0) or 0),
                "min_s": round(min_s, 6),
                "avg_power_w": round(avg_power, 2),
                "avg_gpu_util": float(row.get("avg_gpu_util", 0) or 0),
                "max_mem_mb": float(row.get("max_mem_mb", 0) or 0),
                "num_steady_samples": int(row.get("num_steady_samples", 0) or 0),
                "gpu_energy_j": round(gpu_energy_j, 4),
                "gpu_energy_derived_j": round(derived_gpu_e, 4),
                "avg_cpu_pkg_w": round(avg_cpu_pkg, 2),
                "cpu_energy_j": round(cpu_energy_j, 4),
                "cpu_energy_derived_j": round(derived_cpu_e, 4),
                "total_energy_j": round(gpu_energy_j + cpu_energy_j, 4),
                "status": row.get("status", ""),
                "source_file": path.name,
            })
    return rows


def filter_latest(rows: list[dict]) -> list[dict]:
    """Keep only the latest result per (engine, benchmark, sf, query) combo.

    "Latest" is determined by the source_file timestamp suffix.
    """
    latest: dict[tuple, dict] = {}
    for row in rows:
        key = (row["engine"], row["benchmark"], row["sf"], row["query"])
        ts = parse_timestamp(row["source_file"])
        existing_ts = parse_timestamp(latest[key]["source_file"]) if key in latest else ""
        if ts >= existing_ts:
            latest[key] = row
    return list(latest.values())


def print_summary_table(rows: list[dict]) -> None:
    """Print a grouped summary to stdout."""
    if not rows:
        print("  No data found.")
        return

    # Group by engine -> benchmark -> sf
    grouped: dict[str, dict[str, dict[str, list[dict]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(list))
    )
    for r in rows:
        if r["status"] != "OK":
            continue
        grouped[r["engine"]][r["benchmark"]][r["sf"]].append(r)

    total_gpu_e = 0.0
    total_cpu_e = 0.0
    total_queries = 0

    for engine in sorted(grouped):
        print(f"\n  {'='*66}")
        print(f"  Engine: {engine.upper()}")
        print(f"  {'='*66}")

        for bench in sorted(grouped[engine]):
            for sf in sorted(grouped[engine][bench], key=str):
                qrows = grouped[engine][bench][sf]
                n = len(qrows)
                sum_gpu_e = sum(r["gpu_energy_j"] for r in qrows)
                sum_cpu_e = sum(r["cpu_energy_j"] for r in qrows)
                sum_total = sum_gpu_e + sum_cpu_e
                avg_power = (sum(r["avg_power_w"] for r in qrows) / n) if n else 0
                avg_util = (sum(r["avg_gpu_util"] for r in qrows) / n) if n else 0
                avg_latency_ms = (sum(r["min_s"] * 1000 for r in qrows) / n) if n else 0

                total_gpu_e += sum_gpu_e
                total_cpu_e += sum_cpu_e
                total_queries += n

                print(f"\n  {bench} SF={sf} ({n} queries)")
                print(f"    GPU Energy:   {sum_gpu_e:8.2f} J  "
                      f"(avg {sum_gpu_e/n:.4f} J/query)")
                print(f"    CPU Energy:   {sum_cpu_e:8.2f} J  "
                      f"(avg {sum_cpu_e/n:.4f} J/query)")
                print(f"    Total Energy: {sum_total:8.2f} J  "
                      f"(avg {sum_total/n:.4f} J/query)")
                print(f"    Avg Power:    {avg_power:8.1f} W")
                print(f"    Avg GPU Util: {avg_util:8.1f} %")
                print(f"    Avg Latency:  {avg_latency_ms:8.2f} ms")

                # Show top-5 most energy-hungry queries
                by_energy = sorted(qrows, key=lambda r: r["gpu_energy_j"], reverse=True)
                top = by_energy[:5]
                if top:
                    print(f"    Top queries by GPU energy:")
                    for r in top:
                        print(f"      {r['query']:>12s}: "
                              f"{r['gpu_energy_j']:.4f} J  "
                              f"({r['avg_power_w']:.0f}W × {r['min_s']*1000:.1f}ms)")

    print(f"\n  {'='*66}")
    print(f"  GRAND TOTAL ({total_queries} queries)")
    print(f"    GPU Energy:   {total_gpu_e:8.2f} J")
    print(f"    CPU Energy:   {total_cpu_e:8.2f} J")
    print(f"    Total Energy: {total_gpu_e + total_cpu_e:8.2f} J")
    print(f"  {'='*66}")


def main():
    parser = argparse.ArgumentParser(
        description="Compute energy summary from Category A metrics results",
    )
    parser.add_argument(
        "--results-dir", type=str, default=None,
        help="Results directory (default: <project>/results)",
    )
    parser.add_argument(
        "--latest", action="store_true",
        help="Only use the latest run per (engine, benchmark, sf, query)",
    )
    parser.add_argument(
        "--output", type=str, default=None,
        help="Output CSV path (default: <results-dir>/energy_summary.csv)",
    )
    args = parser.parse_args()

    results_dir = Path(args.results_dir) if args.results_dir else MAXIMUS_DIR / "results"
    if not results_dir.exists():
        print(f"ERROR: results directory not found: {results_dir}")
        sys.exit(1)

    # Find all metrics summary CSVs
    summary_files = find_metrics_summaries(results_dir)
    if not summary_files:
        print(f"No *_metrics_summary_*.csv files found in {results_dir}")
        sys.exit(0)

    print(f"{'='*70}")
    print(f"  ENERGY SUMMARY")
    print(f"  Results dir: {results_dir}")
    print(f"  Found {len(summary_files)} metrics summary files")
    print(f"{'='*70}")

    # Load all rows
    all_rows: list[dict] = []
    for f in summary_files:
        engine = parse_engine(f.name)
        if engine == "maximus":
            all_rows.extend(load_maximus_summary(f))
        elif engine == "sirius":
            all_rows.extend(load_sirius_summary(f))
        else:
            print(f"  [WARN] Unknown engine for {f.name}, skipping")

    print(f"  Loaded {len(all_rows)} query results")

    if args.latest:
        all_rows = filter_latest(all_rows)
        print(f"  After --latest filter: {len(all_rows)} query results")

    # Print summary
    print_summary_table(all_rows)

    # Write CSV
    output_path = Path(args.output) if args.output else results_dir / "energy_summary.csv"
    fields = [
        "engine", "benchmark", "sf", "query", "storage", "n_reps",
        "min_s", "avg_power_w", "avg_gpu_util", "max_mem_mb",
        "num_steady_samples",
        "gpu_energy_j", "gpu_energy_derived_j",
        "avg_cpu_pkg_w", "cpu_energy_j", "cpu_energy_derived_j",
        "total_energy_j", "status", "source_file",
    ]
    with open(output_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        # Sort for readability
        all_rows.sort(key=lambda r: (r["engine"], r["benchmark"], str(r["sf"]), r["query"]))
        w.writerows(all_rows)

    print(f"\n  Output CSV: {output_path} ({len(all_rows)} rows)")


if __name__ == "__main__":
    main()

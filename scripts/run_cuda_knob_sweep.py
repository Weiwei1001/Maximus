#!/usr/bin/env python3
"""
Quick CUDA knob sweep: measure latency impact of software-level CUDA knobs.

Tests on TPC-H SF1 with representative queries:
  - q1  (scan + aggregation, memory-bound)
  - q6  (filter + aggregation, simple)
  - q3  (2-way join)
  - q9  (complex multi-join)

Knobs tested:
  1. Storage device: gpu vs cpu vs cpu-pinned  (pinned memory effect)
  2. Operator fusion: on vs off
  3. Pinned pool size: 1GB, 4GB, 8GB, 12GB
  4. CUDA_DEVICE_MAX_CONNECTIONS: 1, 8, 32
  5. CUDA_AUTO_BOOST: 0, 1
"""
from __future__ import annotations

import csv
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────
MAXIMUS_DIR = Path("/home/xzw/Maximus")
MAXBENCH = MAXIMUS_DIR / "build" / "benchmarks" / "maxbench"
DATA_PATH = MAXIMUS_DIR / "tests" / "tpch" / "csv-1"
RESULTS_DIR = Path("/home/xzw/gpu_db/results/cuda_knob_sweep")

LD_EXTRA = [
    "/home/xzw/Maximus/.venv/lib/python3.12/site-packages/nvidia/libnvcomp/lib64",
    "/home/xzw/Maximus/.venv/lib/python3.12/site-packages/libkvikio/lib64",
]

# Representative queries
QUERIES = ["q1", "q6", "q3", "q9"]
N_REPS = 30  # enough to get stable min


@dataclass
class KnobConfig:
    name: str
    knob: str
    value: str
    env_overrides: dict = field(default_factory=dict)
    cli_overrides: list = field(default_factory=list)


def get_base_env():
    env = os.environ.copy()
    ld = env.get("LD_LIBRARY_PATH", "")
    env["LD_LIBRARY_PATH"] = ":".join(LD_EXTRA) + (":" + ld if ld else "")
    return env


def build_configs() -> list[KnobConfig]:
    configs = []

    # ── Knob 1: Storage device (pinned vs pageable vs gpu-resident) ──
    for sd in ["gpu", "cpu", "cpu-pinned"]:
        configs.append(KnobConfig(
            name=f"storage_{sd}",
            knob="storage_device",
            value=sd,
            cli_overrides=["-s", sd],
        ))

    # ── Knob 2: Operator fusion on/off ──
    for fusion in ["true", "false"]:
        configs.append(KnobConfig(
            name=f"fusion_{fusion}",
            knob="operator_fusion",
            value=fusion,
            env_overrides={"MAXIMUS_OPERATORS_FUSION": fusion},
        ))

    # ── Knob 3: Pinned pool size ──
    for gb in [1, 4, 8, 12]:
        configs.append(KnobConfig(
            name=f"pinned_pool_{gb}gb",
            knob="pinned_pool_size_gb",
            value=str(gb),
            env_overrides={"MAXIMUS_MAX_PINNED_POOL_SIZE": str(gb * 1024 * 1024 * 1024)},
        ))

    # ── Knob 4: CUDA_DEVICE_MAX_CONNECTIONS ──
    for n in [1, 8, 32]:
        configs.append(KnobConfig(
            name=f"max_conn_{n}",
            knob="CUDA_DEVICE_MAX_CONNECTIONS",
            value=str(n),
            env_overrides={"CUDA_DEVICE_MAX_CONNECTIONS": str(n)},
        ))

    # ── Knob 5: CUDA_AUTO_BOOST ──
    for v in ["0", "1"]:
        configs.append(KnobConfig(
            name=f"auto_boost_{v}",
            knob="CUDA_AUTO_BOOST",
            value=v,
            env_overrides={"CUDA_AUTO_BOOST": v},
        ))

    return configs


def parse_timings(output: str) -> dict[str, list[int]]:
    """Extract per-query timing lists from maxbench output."""
    result = {}
    current_query = None
    for line in output.split("\n"):
        qm = re.match(r"\s*QUERY (\w+)\s*", line.strip())
        if qm:
            current_query = qm.group(1)
        tm = re.match(r"- MAXIMUS TIMINGS \[ms\]:\s*(.*)", line.strip())
        if tm and current_query:
            ts = tm.group(1).strip().rstrip(",")
            times = [int(t.strip()) for t in ts.split(",") if t.strip()]
            result[current_query] = times
    # fallback: csv summary lines
    for line in output.split("\n"):
        if line.startswith("gpu,maximus,"):
            parts = line.strip().split(",")
            if len(parts) >= 4:
                qname = parts[2]
                times = [int(t) for t in parts[3:] if t.strip()]
                if qname not in result:
                    result[qname] = times
    return result


def run_experiment(config: KnobConfig) -> dict[str, dict]:
    """Run maxbench with given config, return {query: {min_ms, avg_ms, times}}."""
    env = get_base_env()
    env.update(config.env_overrides)

    # Default CLI: gpu device, gpu storage, 30 reps
    storage = "gpu"
    cli = [
        str(MAXBENCH),
        "--benchmark", "tpch",
        "-q", ",".join(QUERIES),
        "-d", "gpu",
        "-r", str(N_REPS),
        "--n_reps_storage", "1",
        "--path", str(DATA_PATH),
        "-s", storage,
        "--engines", "maximus",
    ]

    # Apply CLI overrides (e.g., storage device)
    if config.cli_overrides:
        # Replace -s value if storage_device knob
        if "-s" in config.cli_overrides:
            idx = cli.index("-s")
            cli[idx + 1] = config.cli_overrides[config.cli_overrides.index("-s") + 1]

    try:
        proc = subprocess.run(
            cli, capture_output=True, text=True, timeout=300, env=env
        )
        output = proc.stdout + (proc.stderr or "")
    except subprocess.TimeoutExpired:
        return {q: {"min_ms": -1, "avg_ms": -1, "status": "timeout"} for q in QUERIES}
    except Exception as e:
        return {q: {"min_ms": -1, "avg_ms": -1, "status": str(e)} for q in QUERIES}

    timings = parse_timings(output)
    results = {}
    for q in QUERIES:
        if q in timings and timings[q]:
            t = timings[q]
            results[q] = {
                "min_ms": min(t),
                "avg_ms": round(sum(t) / len(t), 1),
                "all_ms": t,
                "status": "ok",
            }
        else:
            results[q] = {"min_ms": -1, "avg_ms": -1, "all_ms": [], "status": "missing"}

    return results


def main():
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    configs = build_configs()

    print(f"CUDA Knob Sweep: {len(configs)} configurations × {len(QUERIES)} queries")
    print(f"Queries: {QUERIES}, Reps: {N_REPS}")
    print(f"Data: {DATA_PATH}")
    print(f"Results: {RESULTS_DIR}")
    print("=" * 70)

    all_rows = []

    for i, cfg in enumerate(configs):
        print(f"\n[{i+1}/{len(configs)}] {cfg.name} ({cfg.knob}={cfg.value})")
        sys.stdout.flush()

        t0 = time.perf_counter()
        results = run_experiment(cfg)
        wall = time.perf_counter() - t0

        for q, r in results.items():
            row = {
                "config_name": cfg.name,
                "knob": cfg.knob,
                "value": cfg.value,
                "query": q,
                "min_ms": r["min_ms"],
                "avg_ms": r["avg_ms"],
                "status": r["status"],
            }
            all_rows.append(row)
            status_icon = "✓" if r["status"] == "ok" else "✗"
            print(f"  {q}: min={r['min_ms']}ms  avg={r['avg_ms']}ms  {status_icon}")

        print(f"  wall time: {wall:.1f}s")

    # Write CSV
    out_csv = RESULTS_DIR / "knob_sweep_summary.csv"
    with open(out_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "config_name", "knob", "value", "query", "min_ms", "avg_ms", "status"
        ])
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"\n{'=' * 70}")
    print(f"Results written to {out_csv}")

    # Print summary table
    print(f"\n{'='*70}")
    print("SUMMARY (min_ms)")
    print(f"{'Config':<25} {'q1':>6} {'q6':>6} {'q3':>6} {'q9':>6}")
    print("-" * 55)
    for cfg in configs:
        vals = []
        for q in QUERIES:
            matching = [r for r in all_rows if r["config_name"] == cfg.name and r["query"] == q]
            vals.append(str(matching[0]["min_ms"]) if matching else "?")
        print(f"{cfg.name:<25} {vals[0]:>6} {vals[1]:>6} {vals[2]:>6} {vals[3]:>6}")


if __name__ == "__main__":
    main()

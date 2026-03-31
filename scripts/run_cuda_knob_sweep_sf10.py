#!/usr/bin/env python3
"""
CUDA knob sweep on TPC-H SF10 — larger dataset to expose transfer/memory effects.
Only tests the knobs that showed potential or need larger data to differentiate.
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

MAXIMUS_DIR = Path("/home/xzw/Maximus")
MAXBENCH = MAXIMUS_DIR / "build" / "benchmarks" / "maxbench"
DATA_PATH = MAXIMUS_DIR / "tests" / "tpch" / "csv-10"
RESULTS_DIR = Path("/home/xzw/gpu_db/results/cuda_knob_sweep")

LD_EXTRA = [
    "/home/xzw/Maximus/.venv/lib/python3.12/site-packages/nvidia/libnvcomp/lib64",
    "/home/xzw/Maximus/.venv/lib/python3.12/site-packages/libkvikio/lib64",
]

# Larger dataset: use fewer queries but more representative
# q1=scan+agg, q6=filter+agg, q3=join, q9=complex join, q12=join+agg
QUERIES = ["q1", "q6", "q3", "q9", "q12"]
N_REPS = 10  # fewer reps for SF10 (each run is slower)


@dataclass
class KnobConfig:
    name: str
    knob: str
    value: str
    storage: str = "gpu"
    env_overrides: dict = field(default_factory=dict)


def get_env(extra: dict = None):
    env = os.environ.copy()
    ld = env.get("LD_LIBRARY_PATH", "")
    env["LD_LIBRARY_PATH"] = ":".join(LD_EXTRA) + (":" + ld if ld else "")
    if extra:
        env.update(extra)
    return env


def build_configs() -> list[KnobConfig]:
    configs = []

    # ── Knob 1: Storage device (THE big knob from SF1 results) ──
    for sd in ["gpu", "cpu", "cpu-pinned"]:
        configs.append(KnobConfig(
            name=f"storage_{sd}", knob="storage_device", value=sd, storage=sd,
        ))

    # ── Knob 2: Operator fusion (re-test with larger data) ──
    for fusion in ["true", "false"]:
        configs.append(KnobConfig(
            name=f"fusion_{fusion}", knob="operator_fusion", value=fusion,
            env_overrides={"MAXIMUS_OPERATORS_FUSION": fusion},
        ))

    # ── Knob 3: Pinned pool size (matters more with cpu-pinned storage) ──
    for gb in [1, 4, 8, 12]:
        configs.append(KnobConfig(
            name=f"pinned_{gb}gb_cpupin", knob="pinned_pool_gb", value=str(gb),
            storage="cpu-pinned",
            env_overrides={"MAXIMUS_MAX_PINNED_POOL_SIZE": str(gb * 1024**3)},
        ))

    # ── Knob 4: CUDA_DEVICE_MAX_CONNECTIONS with cpu storage ──
    # (more relevant when there's actual H2D transfer happening)
    for n in [1, 8, 32]:
        configs.append(KnobConfig(
            name=f"conn_{n}_cpustor", knob="CUDA_DEVICE_MAX_CONNECTIONS", value=str(n),
            storage="cpu",
            env_overrides={"CUDA_DEVICE_MAX_CONNECTIONS": str(n)},
        ))

    return configs


def parse_timings(output: str) -> dict[str, list[int]]:
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
    for line in output.split("\n"):
        if line.startswith("gpu,maximus,"):
            parts = line.strip().split(",")
            if len(parts) >= 4:
                qname = parts[2]
                times = [int(t) for t in parts[3:] if t.strip()]
                if qname not in result:
                    result[qname] = times
    return result


def run_one(config: KnobConfig) -> dict[str, dict]:
    env = get_env(config.env_overrides)
    cmd = [
        str(MAXBENCH),
        "--benchmark", "tpch",
        "-q", ",".join(QUERIES),
        "-d", "gpu",
        "-r", str(N_REPS),
        "--n_reps_storage", "1",
        "--path", str(DATA_PATH),
        "-s", config.storage,
        "--engines", "maximus",
    ]

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=600, env=env)
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
            results[q] = {"min_ms": min(t), "avg_ms": round(sum(t)/len(t), 1), "status": "ok"}
        else:
            results[q] = {"min_ms": -1, "avg_ms": -1, "status": "missing"}
    return results


def main():
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    configs = build_configs()

    print(f"CUDA Knob Sweep SF10: {len(configs)} configs × {len(QUERIES)} queries × {N_REPS} reps")
    print(f"Data: {DATA_PATH} (~11GB)")
    print("=" * 70)

    all_rows = []
    for i, cfg in enumerate(configs):
        print(f"\n[{i+1}/{len(configs)}] {cfg.name} (storage={cfg.storage}, {cfg.knob}={cfg.value})")
        sys.stdout.flush()

        t0 = time.perf_counter()
        results = run_one(cfg)
        wall = time.perf_counter() - t0

        for q, r in results.items():
            row = {
                "config_name": cfg.name, "knob": cfg.knob, "value": cfg.value,
                "storage": cfg.storage, "query": q,
                "min_ms": r["min_ms"], "avg_ms": r["avg_ms"], "status": r["status"],
            }
            all_rows.append(row)
            icon = "✓" if r["status"] == "ok" else "✗"
            print(f"  {q}: min={r['min_ms']}ms  avg={r['avg_ms']}ms  {icon}")
        print(f"  wall: {wall:.1f}s")

    out_csv = RESULTS_DIR / "knob_sweep_sf10_summary.csv"
    with open(out_csv, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "config_name", "knob", "value", "storage", "query",
            "min_ms", "avg_ms", "status",
        ])
        w.writeheader()
        w.writerows(all_rows)

    print(f"\n{'='*70}")
    print(f"Results: {out_csv}")
    print(f"\n{'Config':<25} {'q1':>6} {'q6':>6} {'q3':>6} {'q9':>6} {'q12':>6}")
    print("-" * 61)
    for cfg in configs:
        vals = []
        for q in QUERIES:
            m = [r for r in all_rows if r["config_name"] == cfg.name and r["query"] == q]
            vals.append(str(m[0]["min_ms"]) if m else "?")
        print(f"{cfg.name:<25} {vals[0]:>6} {vals[1]:>6} {vals[2]:>6} {vals[3]:>6} {vals[4]:>6}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Add Maximus -s cpu (data on CPU) runs to the existing freq sweep.
Runs only the missing maximus_cpu/ data for each config.
"""
from __future__ import annotations

import csv
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

from hw_detect import (
    detect_gpu, detect_cpu, gpu_sm_clock_levels, cpu_freq_levels,
    set_gpu_sm_clock, reset_gpu_clocks, set_cpu_freq, reset_cpu_freq,
)

SCRIPT_DIR = Path(__file__).resolve().parent
MAXIMUS_DIR = SCRIPT_DIR.parent.parent

# Detected at startup in main().
GPU_ID: str = "0"
CONFIGS: list[dict] = []

BENCHMARKS = ["tpch", "h2o", "clickbench"]
TARGET_TIME = 10


def set_freq_config(cfg):
    gpu_id_int = int(GPU_ID)
    if cfg.get("cpu_freq_khz") is not None:
        set_cpu_freq(cfg["cpu_freq_khz"])
    if cfg["gpu_clk"] is not None:
        set_gpu_sm_clock(gpu_id_int, cfg["gpu_clk"])
    else:
        reset_gpu_clocks(gpu_id_int)
    time.sleep(2)
    gpu_clk_str = f"{cfg['gpu_clk']}MHz" if cfg['gpu_clk'] else "auto"
    print(f"  [FREQ] CPU freq_khz={cfg.get('cpu_freq_khz', 'max')} | GPU: {gpu_clk_str}")


def restore_defaults():
    reset_cpu_freq()
    reset_gpu_clocks(int(GPU_ID))
    print("  [FREQ] Restored defaults")


def main():
    global GPU_ID, CONFIGS

    # Auto-detect hardware
    gpu_info = detect_gpu()
    cpu_info = detect_cpu()
    GPU_ID = str(gpu_info["index"])
    gpu_low_clk = gpu_sm_clock_levels(gpu_info)[0]
    cpu_low_freq = cpu_freq_levels(cpu_info)[0]
    cpu_max_freq = cpu_info["max_freq_khz"]

    CONFIGS = [
        {"name": "baseline",  "cpu_freq_khz": cpu_max_freq, "gpu_clk": None},
        {"name": "cpu_low",   "cpu_freq_khz": cpu_low_freq, "gpu_clk": None},
        {"name": "gpu_low",   "cpu_freq_khz": cpu_max_freq, "gpu_clk": gpu_low_clk},
        {"name": "both_low",  "cpu_freq_khz": cpu_low_freq, "gpu_clk": gpu_low_clk},
    ]

    results_base = MAXIMUS_DIR / "results" / "freq_sweep"
    total_start = time.time()

    print(f"\n{'='*70}")
    print(f"  FREQ SWEEP — ADD MAXIMUS -s cpu RUNS")
    print(f"  [HW] GPU #{GPU_ID}: {gpu_info['name']}")
    print(f"  Started: {datetime.now()}")
    print(f"{'='*70}\n")

    for idx, cfg in enumerate(CONFIGS):
        cname = cfg["name"]
        cfg_dir = results_base / cname
        cpu_dir = cfg_dir / "maximus_cpu"

        # Skip if already done
        if cpu_dir.exists() and list(cpu_dir.glob("*_metrics_summary_*.csv")):
            print(f"  SKIP {cname}: maximus_cpu/ already has results")
            continue

        cpu_dir.mkdir(parents=True, exist_ok=True)

        print(f"\n{'='*70}")
        print(f"  CONFIG {idx+1}/4: {cname} — Maximus -s cpu")
        print(f"{'='*70}")

        set_freq_config(cfg)

        cmd = [
            sys.executable, str(SCRIPT_DIR / "run_maximus_metrics.py"),
            *BENCHMARKS,
            "--results-dir", str(cpu_dir),
            "--target-time", str(TARGET_TIME),
            "--storage", "cpu",
        ]
        print(f"  [RUN] {' '.join(cmd)}")
        t0 = time.time()
        try:
            r = subprocess.run(cmd, timeout=7200)
            elapsed = time.time() - t0
            print(f"  Maximus -s cpu: {'OK' if r.returncode == 0 else 'FAIL'} in {elapsed/60:.1f}min")
        except subprocess.TimeoutExpired:
            print(f"  Maximus -s cpu: TIMEOUT after {(time.time()-t0)/60:.1f}min")

        print(f"  Cooling 10s...")
        time.sleep(10)

    restore_defaults()

    total_elapsed = time.time() - total_start
    print(f"\n{'='*70}")
    print(f"  COMPLETE — {timedelta(seconds=int(total_elapsed))}")
    print(f"  Finished: {datetime.now()}")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Fix freq_sweep: re-run Sirius for gpu_low and both_low configs,
and Maximus clickbench for gpu_low.
"""
from __future__ import annotations

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
RESULTS_BASE = MAXIMUS_DIR / "results" / "freq_sweep"
TARGET_TIME = 10

# Detected at startup in main().
GPU_ID: str = "0"
CONFIGS_TO_FIX: list[dict] = []


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


def run_metrics(script_name, benchmarks, results_dir, extra_args=None):
    cmd = [
        sys.executable, str(SCRIPT_DIR / script_name),
        *benchmarks,
        "--results-dir", str(results_dir),
        "--target-time", str(TARGET_TIME),
    ]
    if extra_args:
        cmd.extend(extra_args)
    print(f"  [RUN] {' '.join(cmd)}")
    t0 = time.time()
    try:
        r = subprocess.run(cmd, timeout=7200)
        elapsed = time.time() - t0
        return r.returncode == 0, elapsed
    except subprocess.TimeoutExpired:
        return False, time.time() - t0


def main():
    global GPU_ID, CONFIGS_TO_FIX

    # Auto-detect hardware
    gpu_info = detect_gpu()
    cpu_info = detect_cpu()
    GPU_ID = str(gpu_info["index"])
    gpu_low_clk = gpu_sm_clock_levels(gpu_info)[0]
    cpu_low_freq = cpu_freq_levels(cpu_info)[0]
    cpu_max_freq = cpu_info["max_freq_khz"]

    CONFIGS_TO_FIX = [
        {"name": "gpu_low",   "cpu_freq_khz": cpu_max_freq, "gpu_clk": gpu_low_clk},
        {"name": "both_low",  "cpu_freq_khz": cpu_low_freq, "gpu_clk": gpu_low_clk},
    ]

    total_start = time.time()
    print(f"\n{'='*70}")
    print(f"  FREQ SWEEP FIX — re-run failed configs")
    print(f"  [HW] GPU #{GPU_ID}: {gpu_info['name']}, low SM clock={gpu_low_clk}MHz")
    print(f"  Started: {datetime.now()}")
    print(f"{'='*70}\n")

    for cfg in CONFIGS_TO_FIX:
        cname = cfg["name"]
        cfg_dir = RESULTS_BASE / cname

        print(f"\n{'='*70}")
        print(f"  CONFIG: {cname}")
        print(f"{'='*70}")

        set_freq_config(cfg)

        # Re-run Sirius (all benchmarks)
        print(f"\n  --- Sirius metrics for {cname} ---")
        ok, elapsed = run_metrics("run_sirius_metrics.py",
                                  ["tpch", "h2o", "clickbench"],
                                  str(cfg_dir))
        print(f"  Sirius: {'OK' if ok else 'FAIL'} in {elapsed/60:.1f}min")

        # For gpu_low, also run Maximus clickbench (was missing)
        if cname == "gpu_low":
            print(f"\n  --- Maximus clickbench for {cname} ---")
            ok, elapsed = run_metrics("run_maximus_metrics.py",
                                      ["clickbench"],
                                      str(cfg_dir),
                                      ["--storage", "gpu"])
            print(f"  Maximus clickbench: {'OK' if ok else 'FAIL'} in {elapsed/60:.1f}min")

        print(f"  Cooling 10s...")
        time.sleep(10)

    restore_defaults()

    total_elapsed = time.time() - total_start
    print(f"\n{'='*70}")
    print(f"  COMPLETE — {timedelta(seconds=int(total_elapsed))}")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Full frequency sweep: run all benchmarks under 4 CPU/GPU frequency configs.

Uses existing run_maximus_metrics.py and run_sirius_metrics.py scripts,
wrapping them with frequency control via intel_pstate + nvidia-smi.

Configs:
  baseline:  CPU=100%, turbo=on, GPU=unlocked
  cpu_low:   CPU=18% (800MHz), turbo=off, GPU=unlocked
  gpu_low:   CPU=100%, turbo=on, GPU=180MHz
  both_low:  CPU=18% (800MHz), turbo=off, GPU=180MHz

Results: results/freq_sweep/<config_name>/
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
TARGET_TIME = 10  # seconds of sustained execution per query


def set_freq_config(cfg):
    """Apply CPU and GPU frequency settings."""
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


def run_metrics_script(script_name, benchmarks, results_dir, target_time, extra_args=None):
    """Run a metrics script and return (success, elapsed)."""
    cmd = [
        sys.executable, str(SCRIPT_DIR / script_name),
        *benchmarks,
        "--results-dir", str(results_dir),
        "--target-time", str(target_time),
    ]
    if extra_args:
        cmd.extend(extra_args)
    print(f"  [RUN] {' '.join(cmd)}")
    t0 = time.time()
    try:
        r = subprocess.run(cmd, timeout=7200)  # 2h timeout per engine
        elapsed = time.time() - t0
        return r.returncode == 0, elapsed
    except subprocess.TimeoutExpired:
        return False, time.time() - t0


def main():
    global GPU_ID, CONFIGS

    # Auto-detect hardware
    gpu_info = detect_gpu()
    cpu_info = detect_cpu()
    GPU_ID = str(gpu_info["index"])
    gpu_low_clk = gpu_sm_clock_levels(gpu_info)[0]  # lowest SM clock
    cpu_low_freq = cpu_freq_levels(cpu_info)[0]      # lowest CPU freq (kHz)
    cpu_max_freq = cpu_info["max_freq_khz"]
    print(f"  [HW] GPU #{GPU_ID}: {gpu_info['name']}, low SM clock={gpu_low_clk}MHz")
    print(f"  [HW] CPU: {cpu_info['governor']}, low={cpu_low_freq}kHz, max={cpu_max_freq}kHz")

    CONFIGS = [
        {"name": "baseline",  "cpu_freq_khz": cpu_max_freq, "gpu_clk": None},
        {"name": "cpu_low",   "cpu_freq_khz": cpu_low_freq, "gpu_clk": None},
        {"name": "gpu_low",   "cpu_freq_khz": cpu_max_freq, "gpu_clk": gpu_low_clk},
        {"name": "both_low",  "cpu_freq_khz": cpu_low_freq, "gpu_clk": gpu_low_clk},
    ]

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmarks", nargs="*", default=BENCHMARKS)
    parser.add_argument("--configs", nargs="*", default=None,
                        help="Config names to run (default: all)")
    parser.add_argument("--resume", action="store_true",
                        help="Skip configs that already have results")
    parser.add_argument("--target-time", type=int, default=TARGET_TIME)
    parser.add_argument("--test", action="store_true",
                        help="Quick test mode: baseline config only")
    args = parser.parse_args()
    if args.test:
        configs_to_run = [c for c in CONFIGS if c["name"] == "baseline"]
        if not configs_to_run:
            configs_to_run = CONFIGS[:1]

    results_base = MAXIMUS_DIR / "results" / "freq_sweep"
    results_base.mkdir(parents=True, exist_ok=True)

    configs_to_run = CONFIGS
    if args.configs:
        configs_to_run = [c for c in CONFIGS if c["name"] in args.configs]

    log_file = results_base / "sweep.log"
    pid_file = results_base / "sweep.pid"
    pid_file.write_text(str(os.getpid()))

    # Tee output to log
    import io
    class Tee:
        def __init__(self, *files):
            self.files = files
        def write(self, data):
            for f in self.files:
                f.write(data)
                f.flush()
        def flush(self):
            for f in self.files:
                try:
                    f.flush()
                except ValueError:
                    pass

    log_fh = open(log_file, "a")
    sys.stdout = Tee(sys.__stdout__, log_fh)

    total_configs = len(configs_to_run)
    total_start = time.time()

    print(f"\n{'='*70}")
    print(f"  FREQUENCY SWEEP — ALL BENCHMARKS")
    print(f"  Configs: {', '.join(c['name'] for c in configs_to_run)}")
    print(f"  Benchmarks: {', '.join(args.benchmarks)}")
    print(f"  Target time: {args.target_time}s per query")
    print(f"  Resume: {args.resume}")
    print(f"  Started: {datetime.now()}")
    print(f"{'='*70}\n")

    for idx, cfg in enumerate(configs_to_run):
        cname = cfg["name"]
        cfg_dir = results_base / cname

        # Check resume
        if args.resume and cfg_dir.exists():
            existing = list(cfg_dir.rglob("*_metrics_summary_*.csv"))
            if len(existing) >= 6:  # 3 engine modes × 2+ benchmark files
                print(f"\n  SKIP {cname}: already has {len(existing)} result files")
                continue

        cfg_dir.mkdir(parents=True, exist_ok=True)
        elapsed_so_far = time.time() - total_start
        if idx > 0:
            eta = timedelta(seconds=elapsed_so_far / idx * (total_configs - idx))
        else:
            eta = "unknown"

        print(f"\n{'='*70}")
        print(f"  CONFIG {idx+1}/{total_configs}: {cname}")
        print(f"  CPU={cfg['cpu_freq_khz']}kHz, GPU={cfg['gpu_clk'] or 'auto'}MHz")
        print(f"  ETA: {eta}")
        print(f"{'='*70}")

        set_freq_config(cfg)

        # Run Maximus metrics (-s gpu)
        print(f"\n  --- Maximus metrics (-s gpu) for {cname} ---")
        gpu_dir = cfg_dir / "maximus_gpu"
        gpu_dir.mkdir(parents=True, exist_ok=True)
        ok, elapsed = run_metrics_script(
            "run_maximus_metrics.py", args.benchmarks, gpu_dir, args.target_time,
            extra_args=["--storage", "gpu"])
        print(f"  Maximus -s gpu: {'OK' if ok else 'FAIL'} in {elapsed/60:.1f}min")

        # Run Maximus metrics (-s cpu)
        print(f"\n  --- Maximus metrics (-s cpu) for {cname} ---")
        cpu_dir = cfg_dir / "maximus_cpu"
        cpu_dir.mkdir(parents=True, exist_ok=True)
        ok, elapsed = run_metrics_script(
            "run_maximus_metrics.py", args.benchmarks, cpu_dir, args.target_time,
            extra_args=["--storage", "cpu"])
        print(f"  Maximus -s cpu: {'OK' if ok else 'FAIL'} in {elapsed/60:.1f}min")

        # Run Sirius metrics
        print(f"\n  --- Sirius metrics for {cname} ---")
        ok, elapsed = run_metrics_script(
            "run_sirius_metrics.py", args.benchmarks, cfg_dir, args.target_time)
        print(f"  Sirius: {'OK' if ok else 'FAIL'} in {elapsed/60:.1f}min")

        # Cool down
        print(f"  Cooling 10s...")
        time.sleep(10)

    # Restore
    restore_defaults()

    total_elapsed = time.time() - total_start
    print(f"\n{'='*70}")
    print(f"  FREQUENCY SWEEP COMPLETE")
    print(f"  Total time: {timedelta(seconds=int(total_elapsed))}")
    print(f"  Results: {results_base}/")
    print(f"  Finished: {datetime.now()}")
    print(f"{'='*70}")

    # Aggregate summary
    print(f"\n  Aggregating results...")
    all_rows = []
    for cfg in CONFIGS:
        cfg_dir = results_base / cfg["name"]
        if not cfg_dir.exists():
            continue
        # Search in config dir and subdirs (maximus_gpu/, maximus_cpu/)
        for summary_file in sorted(cfg_dir.rglob("*_metrics_summary_*.csv")):
            parent_name = summary_file.parent.name
            if summary_file.name.startswith("maximus"):
                if parent_name == "maximus_gpu":
                    engine = "maximus_gpu"
                elif parent_name == "maximus_cpu":
                    engine = "maximus_cpu"
                else:
                    engine = "maximus_gpu"  # legacy files from previous run
            else:
                engine = "sirius"
            with open(summary_file) as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    row["config"] = cfg["name"]
                    row["cpu_freq_khz"] = cfg["cpu_freq_khz"]
                    row["gpu_clk"] = cfg["gpu_clk"] or "auto"
                    row["engine"] = engine
                    all_rows.append(row)

    if all_rows:
        agg_file = results_base / "freq_sweep_summary.csv"
        # Collect all unique fields across all rows to handle heterogeneous CSVs.
        all_fields = set()
        for row in all_rows:
            all_fields.update(row.keys())
        prefix = ["config", "cpu_freq_khz", "gpu_clk", "engine"]
        rest = sorted(all_fields - set(prefix))
        fields = prefix + rest
        with open(agg_file, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            w.writeheader()
            w.writerows(all_rows)
        print(f"  Summary: {agg_file} ({len(all_rows)} rows)")

    log_fh.close()


if __name__ == "__main__":
    main()

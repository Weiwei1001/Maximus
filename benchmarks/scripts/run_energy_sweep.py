#!/usr/bin/env python3
"""
GPU energy sweep: explore (power-limit, SM-clock) configurations.

Iterates over a grid of GPU power limits and SM clock frequencies, running
the existing Maximus and Sirius metrics scripts at each configuration point.
Results are aggregated into a single energy_sweep_summary.csv for analysis.

Safety: GPU defaults are always restored on exit (including Ctrl+C / exceptions).

Usage:
    python run_energy_sweep.py
    python run_energy_sweep.py --power-limits 250,300,360 --sm-clocks 1200,2400
    python run_energy_sweep.py --engines maximus --benchmarks tpch h2o --resume
    python run_energy_sweep.py --results-dir results/sweep_v2 --resume
"""
from __future__ import annotations

import argparse
import csv
import glob
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

# ── Constants ─────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent

DEFAULT_POWER_LIMITS = [250, 275, 300, 325, 360, 450]
DEFAULT_SM_CLOCKS = [600, 1200, 1800, 2400, 3090]
DEFAULT_POWER_LIMIT = 360  # RTX 5080 default
GPU_ID = "1"

MAXIMUS_BENCHMARKS = {
    "tpch": [1, 2],
    "h2o": ["1gb", "2gb"],
    "clickbench": [5],
}
SIRIUS_BENCHMARKS = {
    "tpch": [1, 2, 5, 10],
    "h2o": ["1gb", "2gb", "3gb", "4gb"],
    "clickbench": [10, 20],
}

# Unified summary CSV columns
SUMMARY_COLUMNS = [
    "power_limit_w", "sm_clock_mhz", "engine", "benchmark", "sf",
    "query", "min_ms", "avg_power_w", "max_power_w", "energy_j",
    "cpu_energy_j", "avg_gpu_util", "status",
]

COOLDOWN_PAUSE_S = 5
MAX_COOLDOWN_TEMP_C = 85


# ══════════════════════════════════════════════════════════════════════════════
#  GPU Configuration Management
# ══════════════════════════════════════════════════════════════════════════════

def set_gpu_config(power_limit_w: int, sm_clock_mhz: int) -> bool:
    """Set GPU power limit and lock SM clocks. Returns True on success."""
    print(f"  [GPU] Setting power limit to {power_limit_w}W, SM clock to {sm_clock_mhz}MHz")

    # Set power limit
    rc1 = subprocess.run(
        ["sudo", "nvidia-smi", "-i", GPU_ID, "-pl", str(power_limit_w)],
        capture_output=True, text=True,
    )
    if rc1.returncode != 0:
        print(f"  [GPU] WARNING: Failed to set power limit: {rc1.stderr.strip()}")
        return False

    # Lock SM clocks
    rc2 = subprocess.run(
        ["sudo", "nvidia-smi", "-i", GPU_ID,
         f"--lock-gpu-clocks={sm_clock_mhz},{sm_clock_mhz}"],
        capture_output=True, text=True,
    )
    if rc2.returncode != 0:
        print(f"  [GPU] WARNING: Failed to lock SM clocks: {rc2.stderr.strip()}")
        return False

    # Verify
    if not verify_gpu_config(power_limit_w, sm_clock_mhz):
        print("  [GPU] WARNING: Verification failed after setting config")
        return False

    print(f"  [GPU] Config applied and verified: PL={power_limit_w}W, CLK={sm_clock_mhz}MHz")
    return True


def restore_gpu_defaults() -> None:
    """Restore GPU to default power limit and unlock clocks."""
    print(f"\n  [GPU] Restoring defaults: PL={DEFAULT_POWER_LIMIT}W, clocks=unlocked")

    subprocess.run(
        ["sudo", "nvidia-smi", "-i", GPU_ID, "-pl", str(DEFAULT_POWER_LIMIT)],
        capture_output=True, text=True,
    )
    subprocess.run(
        ["sudo", "nvidia-smi", "-i", GPU_ID, "--reset-gpu-clocks"],
        capture_output=True, text=True,
    )
    print("  [GPU] Defaults restored")


def verify_gpu_config(expected_pl_w: int, expected_clk_mhz: int) -> bool:
    """Read back GPU settings and verify they match the expected values."""
    r = subprocess.run(
        ["nvidia-smi", "-i", GPU_ID,
         "--query-gpu=power.limit,clocks.sm,clocks.max.sm",
         "--format=csv,noheader,nounits"],
        capture_output=True, text=True, timeout=10,
    )
    if r.returncode != 0:
        return False

    parts = [p.strip() for p in r.stdout.strip().split(",")]
    if len(parts) < 2:
        return False

    try:
        actual_pl = float(parts[0])
        actual_clk = int(float(parts[1]))
    except (ValueError, IndexError):
        return False

    # Power limit: allow 1W tolerance (nvidia-smi may round)
    pl_ok = abs(actual_pl - expected_pl_w) <= 1.0
    # Clock: allow 15 MHz tolerance (driver may adjust slightly)
    clk_ok = abs(actual_clk - expected_clk_mhz) <= 15

    if not pl_ok:
        print(f"  [GPU] PL mismatch: expected {expected_pl_w}W, got {actual_pl}W")
    if not clk_ok:
        print(f"  [GPU] CLK mismatch: expected {expected_clk_mhz}MHz, got {actual_clk}MHz")

    return pl_ok and clk_ok


def get_gpu_temperature() -> float:
    """Query current GPU temperature in Celsius."""
    try:
        r = subprocess.run(
            ["nvidia-smi", "-i", GPU_ID,
             "--query-gpu=temperature.gpu",
             "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=10,
        )
        if r.returncode == 0:
            return float(r.stdout.strip())
    except Exception:
        pass
    return 0.0


def wait_for_cooldown(max_temp: float = MAX_COOLDOWN_TEMP_C) -> None:
    """Block until GPU temperature drops below max_temp."""
    temp = get_gpu_temperature()
    if temp <= max_temp:
        return
    print(f"  [GPU] Temperature {temp:.0f}C > {max_temp:.0f}C, waiting for cooldown...",
          end="", flush=True)
    while temp > max_temp:
        time.sleep(5)
        temp = get_gpu_temperature()
        print(f" {temp:.0f}C", end="", flush=True)
    print(" OK")


def enable_persistence_mode() -> None:
    """Enable nvidia persistence mode to reduce driver overhead."""
    r = subprocess.run(
        ["sudo", "nvidia-smi", "-i", GPU_ID, "-pm", "1"],
        capture_output=True, text=True,
    )
    if r.returncode == 0:
        print("  [GPU] Persistence mode enabled")
    else:
        print(f"  [GPU] WARNING: Could not enable persistence mode: {r.stderr.strip()}")


# ══════════════════════════════════════════════════════════════════════════════
#  Config Tag and Directory Helpers
# ══════════════════════════════════════════════════════════════════════════════

def config_tag(power_limit_w: int, sm_clock_mhz: int) -> str:
    """Generate config tag: pl250w_clk0600mhz"""
    return f"pl{power_limit_w}w_clk{sm_clock_mhz:04d}mhz"


def config_dir(results_dir: Path, power_limit_w: int, sm_clock_mhz: int) -> Path:
    """Get per-config subdirectory under results_dir."""
    return results_dir / config_tag(power_limit_w, sm_clock_mhz)


def config_has_results(cfg_dir: Path, engine: str, benchmarks: list[str]) -> bool:
    """Check if a config directory already has summary CSVs for all requested benchmarks."""
    if not cfg_dir.exists():
        return False
    pattern = f"{engine}_*_metrics_summary_*.csv"
    existing = list(cfg_dir.glob(pattern))
    return len(existing) > 0


# ══════════════════════════════════════════════════════════════════════════════
#  Subprocess Runners for Existing Metrics Scripts
# ══════════════════════════════════════════════════════════════════════════════

def run_maximus_metrics(benchmarks: list[str], cfg_dir: Path,
                        target_time: float) -> int:
    """Run run_maximus_metrics.py as a subprocess. Returns exit code."""
    cmd = [
        sys.executable,
        str(SCRIPT_DIR / "run_maximus_metrics.py"),
        *benchmarks,
        "--results-dir", str(cfg_dir),
        "--target-time", str(target_time),
    ]
    print(f"  [MAXIMUS] Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, timeout=7200)  # 2 hour timeout
    return result.returncode


def run_sirius_metrics(benchmarks: list[str], cfg_dir: Path,
                       target_time: float) -> int:
    """Run run_sirius_metrics.py as a subprocess. Returns exit code."""
    cmd = [
        sys.executable,
        str(SCRIPT_DIR / "run_sirius_metrics.py"),
        *benchmarks,
        "--results-dir", str(cfg_dir),
        "--target-time", str(target_time),
    ]
    print(f"  [SIRIUS] Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, timeout=7200)  # 2 hour timeout
    return result.returncode


# ══════════════════════════════════════════════════════════════════════════════
#  Results Aggregation
# ══════════════════════════════════════════════════════════════════════════════

def parse_maximus_summary(csv_path: Path, power_limit_w: int,
                          sm_clock_mhz: int) -> list[dict]:
    """Parse a Maximus metrics summary CSV into unified rows."""
    rows = []
    try:
        with open(csv_path, newline="") as f:
            reader = csv.DictReader(f)
            for r in reader:
                rows.append({
                    "power_limit_w": power_limit_w,
                    "sm_clock_mhz": sm_clock_mhz,
                    "engine": "maximus",
                    "benchmark": r.get("benchmark", ""),
                    "sf": r.get("sf", ""),
                    "query": r.get("query", ""),
                    "min_ms": r.get("min_ms", ""),
                    "avg_power_w": r.get("avg_power_w", ""),
                    "max_power_w": r.get("max_power_w", ""),
                    "energy_j": r.get("energy_j", ""),
                    "cpu_energy_j": r.get("cpu_energy_j", ""),
                    "avg_gpu_util": r.get("avg_gpu_util", ""),
                    "status": r.get("status", ""),
                })
    except Exception as e:
        print(f"  WARNING: Failed to parse {csv_path}: {e}")
    return rows


def parse_sirius_summary(csv_path: Path, power_limit_w: int,
                         sm_clock_mhz: int) -> list[dict]:
    """Parse a Sirius metrics summary CSV into unified rows.

    Sirius uses min_s (seconds) and gpu_energy_j; we convert to match
    the unified schema (min_ms, energy_j).
    """
    rows = []
    try:
        with open(csv_path, newline="") as f:
            reader = csv.DictReader(f)
            for r in reader:
                # Convert min_s to min_ms
                min_s_str = r.get("min_s", "")
                try:
                    min_ms = float(min_s_str) * 1000 if min_s_str else ""
                    min_ms = f"{min_ms:.1f}" if isinstance(min_ms, float) else ""
                except (ValueError, TypeError):
                    min_ms = ""

                rows.append({
                    "power_limit_w": power_limit_w,
                    "sm_clock_mhz": sm_clock_mhz,
                    "engine": "sirius",
                    "benchmark": r.get("benchmark", ""),
                    "sf": r.get("sf", ""),
                    "query": r.get("query", ""),
                    "min_ms": min_ms,
                    "avg_power_w": r.get("avg_power_w", ""),
                    "max_power_w": r.get("max_power_w", ""),
                    "energy_j": r.get("gpu_energy_j", ""),
                    "cpu_energy_j": r.get("cpu_energy_j", ""),
                    "avg_gpu_util": r.get("avg_gpu_util", ""),
                    "status": r.get("status", ""),
                })
    except Exception as e:
        print(f"  WARNING: Failed to parse {csv_path}: {e}")
    return rows


def aggregate_results(results_dir: Path,
                      power_limits: list[int],
                      sm_clocks: list[int]) -> list[dict]:
    """Read all per-config summary CSVs and combine into unified rows."""
    all_rows = []

    for pl in power_limits:
        for clk in sm_clocks:
            cfg_d = config_dir(results_dir, pl, clk)
            if not cfg_d.exists():
                continue

            # Maximus summaries
            for csv_path in sorted(cfg_d.glob("maximus_*_metrics_summary_*.csv")):
                all_rows.extend(parse_maximus_summary(csv_path, pl, clk))

            # Sirius summaries
            for csv_path in sorted(cfg_d.glob("sirius_*_metrics_summary_*.csv")):
                all_rows.extend(parse_sirius_summary(csv_path, pl, clk))

    return all_rows


def write_sweep_summary(results_dir: Path, rows: list[dict]) -> Path:
    """Write the combined energy_sweep_summary.csv."""
    out_path = results_dir / "energy_sweep_summary.csv"
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=SUMMARY_COLUMNS)
        w.writeheader()
        w.writerows(rows)
    print(f"\n  Sweep summary: {out_path} ({len(rows)} rows)")
    return out_path


def print_best_configs(rows: list[dict]) -> None:
    """Print the lowest-energy configuration per (engine, benchmark, sf)."""
    if not rows:
        print("\n  No results to analyze.")
        return

    # Group by (engine, benchmark, sf)
    groups: dict[tuple, list[dict]] = {}
    for r in rows:
        if r["status"] != "OK" or not r["energy_j"]:
            continue
        try:
            energy = float(r["energy_j"])
        except (ValueError, TypeError):
            continue
        key = (r["engine"], r["benchmark"], str(r["sf"]))
        groups.setdefault(key, []).append((energy, r))

    print(f"\n{'=' * 80}")
    print("  BEST CONFIGURATIONS (lowest total energy per benchmark)")
    print(f"{'=' * 80}")
    print(f"  {'Engine':<10} {'Benchmark':<12} {'SF':<6} {'PL(W)':<8} {'CLK(MHz)':<10} "
          f"{'Avg E(J)':<10} {'Queries'}")
    print(f"  {'-' * 75}")

    for key in sorted(groups.keys()):
        engine, bench, sf = key
        entries = groups[key]

        # Group by config to get total energy per config
        config_energy: dict[tuple, list[float]] = {}
        for energy, r in entries:
            ck = (int(r["power_limit_w"]), int(r["sm_clock_mhz"]))
            config_energy.setdefault(ck, []).append(energy)

        # Find config with lowest average per-query energy
        best_cfg = None
        best_avg = float("inf")
        best_n = 0
        for ck, energies in config_energy.items():
            avg_e = sum(energies) / len(energies)
            if avg_e < best_avg:
                best_avg = avg_e
                best_cfg = ck
                best_n = len(energies)

        if best_cfg:
            pl, clk = best_cfg
            print(f"  {engine:<10} {bench:<12} {sf:<6} {pl:<8} {clk:<10} "
                  f"{best_avg:<10.2f} {best_n}")

    print()


# ══════════════════════════════════════════════════════════════════════════════
#  Main Sweep Loop
# ══════════════════════════════════════════════════════════════════════════════

def run_sweep(args: argparse.Namespace) -> None:
    """Main sweep loop: iterate (PL, CLK), set GPU config, run metrics."""
    results_dir = Path(args.results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)

    power_limits = args.power_limits
    sm_clocks = args.sm_clocks
    engines = args.engines
    benchmarks = args.benchmarks
    resume = args.resume

    total_configs = len(power_limits) * len(sm_clocks)
    total_engine_configs = total_configs * len(engines)

    print("=" * 80)
    print("  GPU ENERGY SWEEP")
    print(f"  Power limits: {power_limits}")
    print(f"  SM clocks:    {sm_clocks}")
    print(f"  Engines:      {engines}")
    print(f"  Benchmarks:   {benchmarks}")
    print(f"  Results dir:  {results_dir}")
    print(f"  Resume:       {resume}")
    print(f"  Total configs: {total_configs} ({total_engine_configs} engine-configs)")
    print(f"  Started: {datetime.now()}")
    print("=" * 80)

    # Enable persistence mode once at the start
    enable_persistence_mode()

    completed = 0
    skipped = 0
    failed = 0
    start_time = time.time()

    for cfg_idx, (pl, clk) in enumerate(
        [(p, c) for p in power_limits for c in sm_clocks], start=1
    ):
        tag = config_tag(pl, clk)
        cfg_d = config_dir(results_dir, pl, clk)

        print(f"\n{'=' * 80}")
        print(f"  CONFIG {cfg_idx}/{total_configs}: {tag} "
              f"(PL={pl}W, CLK={clk}MHz)")

        # ETA calculation
        elapsed = time.time() - start_time
        done = completed + skipped
        if done > 0:
            avg_per_config = elapsed / done
            remaining = total_configs - cfg_idx + 1
            eta_s = avg_per_config * remaining
            eta_str = str(timedelta(seconds=int(eta_s)))
            print(f"  Progress: {done}/{total_configs} done, ETA: {eta_str}")
        print(f"{'=' * 80}")

        # Resume: check if all engines have results for this config
        if resume:
            all_done = True
            for engine in engines:
                if not config_has_results(cfg_d, engine, benchmarks):
                    all_done = False
                    break
            if all_done:
                print(f"  SKIP (resume): {tag} already has results")
                skipped += 1
                continue

        # Cool-down check
        wait_for_cooldown()

        # Set GPU config
        if not set_gpu_config(pl, clk):
            print(f"  FAILED to set GPU config for {tag}, skipping")
            failed += 1
            continue

        # Create config directory
        cfg_d.mkdir(parents=True, exist_ok=True)

        # Run metrics for each engine
        for engine in engines:
            if resume and config_has_results(cfg_d, engine, benchmarks):
                print(f"  SKIP (resume): {engine} already has results for {tag}")
                continue

            # Determine which benchmarks to run for this engine
            if engine == "maximus":
                engine_bench_map = MAXIMUS_BENCHMARKS
            else:
                engine_bench_map = SIRIUS_BENCHMARKS

            bench_to_run = [b for b in benchmarks if b in engine_bench_map]
            if not bench_to_run:
                print(f"  SKIP: No configured benchmarks for {engine}")
                continue

            print(f"\n  --- Running {engine} metrics for {tag} ---")
            try:
                if engine == "maximus":
                    rc = run_maximus_metrics(
                        bench_to_run, cfg_d, args.maximus_target_time)
                else:
                    rc = run_sirius_metrics(
                        bench_to_run, cfg_d, args.sirius_target_time)

                if rc != 0:
                    print(f"  WARNING: {engine} metrics exited with code {rc}")
            except subprocess.TimeoutExpired:
                print(f"  ERROR: {engine} metrics timed out for {tag}")
            except Exception as e:
                print(f"  ERROR: {engine} metrics failed for {tag}: {e}")

        completed += 1

        # Pause between configs for thermal stability
        print(f"\n  Cooling pause ({COOLDOWN_PAUSE_S}s)...")
        time.sleep(COOLDOWN_PAUSE_S)

    # Aggregate results
    print(f"\n{'=' * 80}")
    print("  AGGREGATING RESULTS")
    print(f"{'=' * 80}")

    all_rows = aggregate_results(results_dir, power_limits, sm_clocks)
    if all_rows:
        write_sweep_summary(results_dir, all_rows)
        print_best_configs(all_rows)
    else:
        print("  No results found to aggregate.")

    # Final summary
    elapsed = time.time() - start_time
    print(f"\n{'=' * 80}")
    print(f"  SWEEP COMPLETE")
    print(f"  Total time:  {timedelta(seconds=int(elapsed))}")
    print(f"  Completed:   {completed}/{total_configs}")
    print(f"  Skipped:     {skipped}")
    print(f"  Failed:      {failed}")
    print(f"  Finished:    {datetime.now()}")
    print(f"{'=' * 80}")


# ══════════════════════════════════════════════════════════════════════════════
#  CLI
# ══════════════════════════════════════════════════════════════════════════════

def parse_int_list(s: str) -> list[int]:
    """Parse comma-separated integers."""
    return [int(x.strip()) for x in s.split(",")]


def main():
    parser = argparse.ArgumentParser(
        description="GPU energy sweep: explore (power-limit, SM-clock) configurations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_energy_sweep.py
  python run_energy_sweep.py --power-limits 250,300,360 --sm-clocks 1200,2400
  python run_energy_sweep.py --engines maximus --benchmarks tpch h2o --resume
  python run_energy_sweep.py --results-dir results/sweep_v2 --resume
""",
    )
    parser.add_argument(
        "--power-limits", type=parse_int_list,
        default=DEFAULT_POWER_LIMITS,
        help=f"Comma-separated power limits in watts "
             f"(default: {','.join(map(str, DEFAULT_POWER_LIMITS))})",
    )
    parser.add_argument(
        "--sm-clocks", type=parse_int_list,
        default=DEFAULT_SM_CLOCKS,
        help=f"Comma-separated SM clock frequencies in MHz "
             f"(default: {','.join(map(str, DEFAULT_SM_CLOCKS))})",
    )
    parser.add_argument(
        "--engines", nargs="+", default=["maximus", "sirius"],
        choices=["maximus", "sirius"],
        help="Engines to benchmark (default: maximus sirius)",
    )
    parser.add_argument(
        "--benchmarks", nargs="+",
        default=["tpch", "h2o", "clickbench"],
        choices=["tpch", "h2o", "clickbench"],
        help="Benchmarks to run (default: tpch h2o clickbench)",
    )
    parser.add_argument(
        "--results-dir", type=str,
        default="results/energy_sweep",
        help="Output directory (default: results/energy_sweep)",
    )
    parser.add_argument(
        "--resume", action="store_true",
        help="Skip configs that already have summary CSVs",
    )
    parser.add_argument(
        "--maximus-target-time", type=float, default=10,
        help="Target sustained time for Maximus in seconds (default: 10)",
    )
    parser.add_argument(
        "--sirius-target-time", type=float, default=60,
        help="Target sustained time for Sirius in seconds (default: 60)",
    )
    args = parser.parse_args()

    # Safety: always restore GPU defaults on exit
    try:
        run_sweep(args)
    except KeyboardInterrupt:
        print("\n\n  INTERRUPTED by user (Ctrl+C)")
    except Exception as e:
        print(f"\n\n  FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        restore_gpu_defaults()


if __name__ == "__main__":
    main()

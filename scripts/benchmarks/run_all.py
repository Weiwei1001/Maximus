#!/usr/bin/env python3
"""
End-to-end benchmark runner: generate data + run timing + run metrics.

This is the convenience script that runs the full pipeline.

Usage:
    python run_all.py --maximus-dir /path/to/Maximus --data-dir /path/to/data
    python run_all.py --maximus-dir /path/to/Maximus --data-dir /path/to/data --skip-datagen
"""
import argparse
import subprocess
import sys
import time
from pathlib import Path


def run_script(script: str, args: list):
    """Run a Python script with arguments."""
    cmd = [sys.executable, script] + args
    print(f"\n>>> {' '.join(cmd)}\n")
    result = subprocess.run(cmd, cwd=str(Path(script).parent))
    if result.returncode != 0:
        print(f"ERROR: {script} failed with exit code {result.returncode}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="End-to-end Maximus benchmark pipeline")
    parser.add_argument("--maximus-dir", type=str, required=True, help="Path to Maximus repository")
    parser.add_argument("--data-dir", type=str, required=True, help="Base data directory")
    parser.add_argument("--output-dir", type=str, default=None, help="Output directory (default: data-dir)")
    parser.add_argument("--n-reps", type=int, default=3, help="Number of repetitions (default: 3)")
    parser.add_argument("--storage-device", type=str, default="gpu", choices=["cpu", "gpu"])
    parser.add_argument("--sample-interval", type=int, default=50, help="GPU sampling interval ms")
    parser.add_argument("--skip-datagen", action="store_true", help="Skip data generation")
    parser.add_argument("--skip-timing", action="store_true", help="Skip timing benchmarks")
    parser.add_argument("--skip-metrics", action="store_true", help="Skip metrics benchmarks")
    parser.add_argument("--benchmarks", type=str, nargs="+", default=["tpch", "h2o", "clickbench"],
                        choices=["tpch", "h2o", "clickbench"])
    parser.add_argument("--clickbench-parquet", type=str, default=None,
                        help="Path to hits.parquet for ClickBench")
    # TPC-H scale factors
    parser.add_argument("--tpch-scales", type=int, nargs="+", default=[1, 2, 10, 20])
    parser.add_argument("--h2o-scales", type=int, nargs="+", default=[1, 2, 3, 4])
    parser.add_argument("--click-percentages", type=int, nargs="+", default=[10, 20])
    args = parser.parse_args()

    scripts_dir = Path(__file__).parent
    data_dir = Path(args.data_dir)
    output_dir = Path(args.output_dir) if args.output_dir else data_dir

    print("=" * 60)
    print("  Maximus Full Benchmark Pipeline")
    print(f"  Start: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Data dir: {data_dir}")
    print(f"  Output dir: {output_dir}")
    print("=" * 60)

    # Step 1: Generate data
    if not args.skip_datagen:
        print("\n" + "=" * 60)
        print("  STEP 1: Data Generation")
        print("=" * 60)

        if "tpch" in args.benchmarks:
            run_script(str(scripts_dir / "generate_tpch_data.py"), [
                "--output-dir", str(data_dir / "tpch"),
                "--scale-factors"] + [str(s) for s in args.tpch_scales])

        if "h2o" in args.benchmarks:
            run_script(str(scripts_dir / "generate_h2o_data.py"), [
                "--output-dir", str(data_dir / "h2o"),
                "--scales"] + [str(s) for s in args.h2o_scales])

        if "clickbench" in args.benchmarks:
            cb_args = ["--output-dir", str(data_dir / "clickbench"),
                        "--percentages"] + [str(p) for p in args.click_percentages]
            if args.clickbench_parquet:
                cb_args += ["--parquet", args.clickbench_parquet]
            run_script(str(scripts_dir / "generate_clickbench_data.py"), cb_args)

    # Step 2: Run timing
    if not args.skip_timing:
        print("\n" + "=" * 60)
        print("  STEP 2: Timing Benchmarks")
        print("=" * 60)
        run_script(str(scripts_dir / "run_timing.py"), [
            "--maximus-dir", args.maximus_dir,
            "--data-dir", str(data_dir),
            "--output-dir", str(output_dir),
            "--n-reps", str(args.n_reps),
            "--storage-device", args.storage_device,
            "--benchmarks"] + args.benchmarks)

    # Step 3: Run metrics
    if not args.skip_metrics:
        print("\n" + "=" * 60)
        print("  STEP 3: Metrics Benchmarks")
        print("=" * 60)
        run_script(str(scripts_dir / "run_metrics.py"), [
            "--maximus-dir", args.maximus_dir,
            "--data-dir", str(data_dir),
            "--output-dir", str(output_dir),
            "--n-reps", str(args.n_reps),
            "--sample-interval", str(args.sample_interval),
            "--storage-device", args.storage_device,
            "--benchmarks"] + args.benchmarks)

    print(f"\n{'=' * 60}")
    print(f"  Pipeline complete: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"\n  Output files:")
    for bench in args.benchmarks:
        print(f"    {bench}_timing.csv          - Query timing results")
        print(f"    {bench}_metrics_timings.csv  - Per-query timing with metrics")
        print(f"    {bench}_metrics_samples.csv  - GPU metric samples")
        print(f"    {bench}_raw_*.txt            - Raw maxbench output")
    print("=" * 60)


if __name__ == "__main__":
    main()

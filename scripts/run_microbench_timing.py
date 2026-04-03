#!/usr/bin/env python3
"""Run all microbench timing on updated scale factors. GPU-resident only, 5 reps."""
import csv, os, re, subprocess, sys, time
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "benchmarks" / "scripts"))
from hw_detect import detect_gpu, get_benchmark_config, maximus_data_dir, MAXIMUS_DIR

MAXBENCH = MAXIMUS_DIR / "build" / "benchmarks" / "maxbench"
_conda = Path(os.path.expanduser("~/miniconda3/envs/maximus_gpu/lib"))
_arrow = Path(os.path.expanduser("~/arrow_install/lib"))
LD = ":".join(filter(None, [
    str(_conda) if _conda.exists() else "",
    str(_arrow) if _arrow.exists() else "",
    os.environ.get("LD_LIBRARY_PATH", ""),
]))

GPU = detect_gpu()
CFG = get_benchmark_config(GPU["vram_mb"])
N_REPS = 5
RESULTS_DIR = MAXIMUS_DIR / "results" / "microbench_timing"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)


def get_env():
    env = os.environ.copy()
    env["LD_LIBRARY_PATH"] = LD
    return env


def run_maxbench(bench, queries, n_reps, data_path, timeout=600):
    cmd = [str(MAXBENCH), "--benchmark", bench, "-q", ",".join(queries),
           "-d", "gpu", "-r", str(n_reps), "--n_reps_storage", "1",
           "--path", str(data_path), "-s", "gpu", "--engines", "maximus"]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=get_env())
        return r.stdout + (r.stderr or ""), r.returncode
    except subprocess.TimeoutExpired:
        return "TIMEOUT", -1
    except Exception as e:
        return str(e), -2


def parse_output(output):
    result = {}
    for line in output.split("\n"):
        if line.startswith("gpu,maximus,"):
            parts = line.strip().split(",")
            if len(parts) >= 4:
                q = parts[2]
                times = [float(t) for t in parts[3:] if t.strip()]
                result[q] = times
    # Fallback
    cur = None
    for line in output.split("\n"):
        m = re.match(r"\s*QUERY (\w+)\s*", line.strip())
        if m: cur = m.group(1)
        m2 = re.match(r"- MAXIMUS TIMINGS \[ms\]:\s*(.*)", line.strip())
        if m2 and cur and cur not in result:
            result[cur] = [float(t.strip()) for t in m2.group(1).rstrip(",").split(",") if t.strip()]
    return result


def main():
    benchmarks = ["microbench_tpch", "microbench_h2o", "microbench_clickbench"]
    all_rows = []

    for bench in benchmarks:
        cfg = CFG[bench]
        queries = cfg["queries"]
        for sf in cfg["scale_factors"]:
            tag = f"{bench}_sf{sf}"
            out_file = RESULTS_DIR / f"{tag}.csv"
            if out_file.exists():
                print(f"[SKIP] {tag}")
                with open(out_file) as f:
                    all_rows.extend(list(csv.DictReader(f)))
                continue

            data_path = maximus_data_dir(bench, sf)
            if not data_path.exists():
                print(f"[SKIP] {tag}: no data at {data_path}")
                continue

            print(f"\n{'='*60}")
            print(f"  {tag} ({len(queries)} queries, {N_REPS} reps)")
            print(f"  {datetime.now().strftime('%H:%M:%S')}")
            print(f"{'='*60}")

            # Run all queries together
            output, rc = run_maxbench(bench, queries, N_REPS, data_path,
                                      timeout=max(600, 60 * len(queries)))
            parsed = parse_output(output)

            # Retry missing individually
            for q in queries:
                if q not in parsed:
                    o2, _ = run_maxbench(bench, [q], N_REPS, data_path, timeout=120)
                    p2 = parse_output(o2)
                    if q in p2:
                        parsed[q] = p2[q]

            rows = []
            ok = 0
            sub1ms = 0
            for q in queries:
                times = parsed.get(q, [])
                status = "OK" if times else "FAIL"
                mn = min(times) if times else 0
                if times:
                    ok += 1
                    if mn < 1.0:
                        sub1ms += 1
                        print(f"  {q}: min={mn:.3f}ms  *** <1ms ***")
                    else:
                        print(f"  {q}: min={mn:.2f}ms")
                else:
                    print(f"  {q}: FAIL")
                rows.append({
                    "benchmark": bench, "sf": sf, "query": q,
                    "n_reps": len(times),
                    "min_ms": f"{mn:.4f}" if mn else "",
                    "avg_ms": f"{sum(times)/len(times):.4f}" if times else "",
                    "all_ms": ";".join(f"{t:.4f}" for t in times),
                    "status": status,
                })
            print(f"  --- {ok}/{len(queries)} OK, {sub1ms} sub-1ms")

            fields = ["benchmark", "sf", "query", "n_reps", "min_ms", "avg_ms", "all_ms", "status"]
            with open(out_file, "w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=fields)
                w.writeheader()
                w.writerows(rows)
            print(f"  [SAVED] {out_file}")
            all_rows.extend(rows)

    # Merge
    merged = RESULTS_DIR / "microbench_timing_all.csv"
    fields = ["benchmark", "sf", "query", "n_reps", "min_ms", "avg_ms", "all_ms", "status"]
    with open(merged, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(all_rows)

    total = len(all_rows)
    ok_total = sum(1 for r in all_rows if r["status"] == "OK")
    sub1 = sum(1 for r in all_rows if r["status"] == "OK" and r["min_ms"] and float(r["min_ms"]) < 1.0)
    print(f"\n{'='*60}")
    print(f"  COMPLETE: {datetime.now()}")
    print(f"  Total: {total} rows, {ok_total} OK, {sub1} sub-1ms queries")
    print(f"  Merged: {merged}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()

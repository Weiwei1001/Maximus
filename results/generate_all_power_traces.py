#!/usr/bin/env python3
"""
Generate power trace PNGs for ALL queries across all benchmarks/SFs.
Each PNG: 2x2 grid (Sirius/Maximus) x (GPU-data/CPU-data).
Filename includes n_reps for each scenario.

Query names are normalized: q01 → q1, q02 → q2, etc.
Output: /home/xzw/gpu_db/results/power_traces/
"""
import csv
import glob
import re
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

RESULTS = Path("/home/xzw/gpu_db/results")
OUT_DIR = RESULTS / "power_traces"
OUT_DIR.mkdir(exist_ok=True)


# ── Normalize query names: q01 → q1, q02 → q2 ──

def normalize_query(q):
    """q01 → q1, q02 → q2, q0 stays q0, q10 stays q10."""
    m = re.match(r'^(q)0*(\d+)$', q)
    if m:
        return m.group(1) + m.group(2)
    return q


# ── Load ALL sample data, re-key by normalized run_id ──

def parse_run_id(rid):
    """Parse run_id → (bench, sf, is_cpu, query).
    Examples:
      tpch_sf1_q01       → (tpch, 1, False, q01)
      tpch_sf1_cpu_q01   → (tpch, 1, True, q01)
      clickbench_sf10_q0 → (clickbench, 10, False, q0)
      h2o_sf1gb_cpu_q1   → (h2o, 1gb, True, q1)
    """
    parts = rid.split("_")
    for i, p in enumerate(parts):
        if p.startswith("sf"):
            bench = "_".join(parts[:i])
            sf = p[2:]
            rest = parts[i+1:]
            if rest and rest[0] == "cpu":
                return bench, sf, True, "_".join(rest[1:])
            else:
                return bench, sf, False, "_".join(rest)
    return None


def load_all_samples_normalized(fpath):
    """Load samples, group by normalized (bench, sf, is_cpu, norm_query) key."""
    raw = defaultdict(list)
    with open(fpath) as f:
        for r in csv.DictReader(f):
            raw[r["run_id"]].append({
                "t": float(r["time_offset_ms"]) / 1000.0,
                "gpu_w": float(r["power_w"]),
                "gpu_util": float(r["gpu_util_pct"]),
                "mem_mb": float(r["mem_used_mb"]),
                "cpu_w": float(r["cpu_pkg_power_w"]),
            })
    # Re-key with original run_id for lookup, but also build a normalized mapping
    return raw


def load_all_summaries_raw(fpath):
    """Load summary CSV keyed by original run_id."""
    data = {}
    with open(fpath) as f:
        for r in csv.DictReader(f):
            data[r["run_id"]] = r
    return data


print("Loading sample data...")

# Sirius GPU-data
sirius_gpu_samples_raw = {}
for fp in sorted(glob.glob(str(RESULTS / "sirius_*_metrics_samples_*.csv"))):
    if "cpu_data" in fp:
        continue
    sirius_gpu_samples_raw.update(load_all_samples_normalized(fp))
print(f"  Sirius GPU-data: {len(sirius_gpu_samples_raw)} run_ids")

sirius_gpu_summary_raw = {}
for fp in sorted(glob.glob(str(RESULTS / "sirius_*_metrics_summary_*.csv"))):
    if "cpu_data" in fp:
        continue
    sirius_gpu_summary_raw.update(load_all_summaries_raw(fp))

# Sirius CPU-data
sirius_cpu_samples_raw = load_all_samples_normalized(
    RESULTS / "sirius_cpu_data_samples_20260301_034912.csv"
)
print(f"  Sirius CPU-data: {len(sirius_cpu_samples_raw)} run_ids")

sirius_cpu_summary_raw = {}
with open(RESULTS / "sirius_cpu_data_analysis.csv") as f:
    for r in csv.DictReader(f):
        rid = f"{r['benchmark']}_sf{r['sf']}_cpu_{r['query']}"
        sirius_cpu_summary_raw[rid] = r

# Maximus GPU-data
maximus_gpu_samples_raw = {}
for fp in sorted(glob.glob(str(RESULTS / "maximus_*_metrics_samples_*.csv"))):
    if "cpu_data" in fp:
        continue
    maximus_gpu_samples_raw.update(load_all_samples_normalized(fp))
print(f"  Maximus GPU-data: {len(maximus_gpu_samples_raw)} run_ids")

maximus_gpu_summary_raw = {}
for fp in sorted(glob.glob(str(RESULTS / "maximus_*_metrics_summary_*.csv"))):
    if "cpu_data" in fp:
        continue
    maximus_gpu_summary_raw.update(load_all_summaries_raw(fp))

# Maximus CPU-data
maximus_cpu_samples_raw = load_all_samples_normalized(
    RESULTS / "maximus_cpu_data_metrics_samples_20260301_061838.csv"
)
print(f"  Maximus CPU-data: {len(maximus_cpu_samples_raw)} run_ids")

maximus_cpu_summary_raw = load_all_summaries_raw(
    RESULTS / "maximus_cpu_data_metrics_summary_20260301_061838.csv"
)


# ── Build unified lookup: (bench, sf, norm_query) → data for each scenario ──

def build_lookup(raw_samples, raw_summaries):
    """Build {(bench, sf, norm_query): (samples, nreps, orig_rid)}."""
    lookup = {}
    for rid, samples in raw_samples.items():
        parsed = parse_run_id(rid)
        if not parsed:
            continue
        bench, sf, is_cpu, query = parsed
        nq = normalize_query(query)
        key = (bench, sf, nq)
        nreps = None
        if rid in raw_summaries:
            nr = raw_summaries[rid].get("n_reps")
            if nr:
                nreps = int(nr)
        lookup[key] = (samples, nreps, rid)
    return lookup

# Also handle summaries that have data but no samples (for nreps info)
def build_summary_lookup(raw_summaries):
    lookup = {}
    for rid, summ in raw_summaries.items():
        parsed = parse_run_id(rid)
        if not parsed:
            continue
        bench, sf, is_cpu, query = parsed
        nq = normalize_query(query)
        key = (bench, sf, nq)
        nr = summ.get("n_reps")
        nreps = int(nr) if nr else None
        lookup[key] = nreps
    return lookup


lk_sirius_gpu = build_lookup(sirius_gpu_samples_raw, sirius_gpu_summary_raw)
lk_sirius_cpu = build_lookup(sirius_cpu_samples_raw, sirius_cpu_summary_raw)
lk_maximus_gpu = build_lookup(maximus_gpu_samples_raw, maximus_gpu_summary_raw)
lk_maximus_cpu = build_lookup(maximus_cpu_samples_raw, maximus_cpu_summary_raw)

# Sirius CPU-data always 5 reps; override nreps
for k in lk_sirius_cpu:
    samples, _, orig_rid = lk_sirius_cpu[k]
    lk_sirius_cpu[k] = (samples, 5, orig_rid)

# Also build summary-only lookups for nreps when samples exist elsewhere
nreps_maximus_cpu = build_summary_lookup(maximus_cpu_summary_raw)


def plot_panel(ax, samples, title):
    """Plot GPU/CPU power + GPU utilization on a single axis."""
    if not samples:
        ax.text(0.5, 0.5, "No Data", transform=ax.transAxes,
                ha="center", va="center", fontsize=14, color="gray")
        ax.set_title(title, fontsize=10, fontweight="bold")
        ax.set_xlabel("Time (s)", fontsize=8)
        return

    t = [s["t"] for s in samples]
    gpu_w = [s["gpu_w"] for s in samples]
    cpu_w = [s["cpu_w"] for s in samples]
    gpu_util = [s["gpu_util"] for s in samples]

    l1, = ax.plot(t, gpu_w, color="tab:red", linewidth=1.2, label="GPU Power")
    l2, = ax.plot(t, cpu_w, color="tab:blue", linewidth=1.2, label="CPU Power")
    ax.set_xlabel("Time (s)", fontsize=8)
    ax.set_ylabel("Power (W)", fontsize=8)
    ax.set_title(title, fontsize=10, fontweight="bold")

    ax2 = ax.twinx()
    l3, = ax2.plot(t, gpu_util, color="tab:green", linewidth=1.0,
                   linestyle="--", alpha=0.6, label="GPU Util%")
    ax2.set_ylim(0, 105)
    ax2.set_ylabel("GPU Util%", fontsize=8, color="tab:green")
    ax2.tick_params(axis="y", labelcolor="tab:green", labelsize=7)
    ax.tick_params(axis="both", labelsize=7)

    avg_gpu = np.mean(gpu_w)
    valid_cpu = [c for c in cpu_w if c > 0]
    avg_cpu = np.mean(valid_cpu) if valid_cpu else 0
    avg_util = np.mean(gpu_util)
    ax.text(0.02, 0.95,
            f"GPU:{avg_gpu:.0f}W  CPU:{avg_cpu:.0f}W  Util:{avg_util:.0f}%",
            transform=ax.transAxes, fontsize=7, va="top",
            bbox=dict(boxstyle="round,pad=0.2", facecolor="wheat", alpha=0.8))

    lines = [l1, l2, l3]
    ax.legend(lines, [l.get_label() for l in lines],
              loc="upper right", fontsize=6)


# ── Collect all unique (bench, sf, norm_query) ──

all_keys = set()
all_keys.update(lk_sirius_gpu.keys())
all_keys.update(lk_sirius_cpu.keys())
all_keys.update(lk_maximus_gpu.keys())
all_keys.update(lk_maximus_cpu.keys())

# Sort: by bench, then sf (numeric where possible), then query number
def sort_key(k):
    bench, sf, q = k
    # SF: try numeric
    try:
        sf_num = float(sf.replace("gb", ""))
    except ValueError:
        sf_num = 0
    # Query: extract number
    m = re.match(r'q(\d+)', q)
    q_num = int(m.group(1)) if m else 0
    return (bench, sf_num, q_num)

sorted_keys = sorted(all_keys, key=sort_key)
total = len(sorted_keys)
print(f"\nTotal unique (bench, sf, query) after normalization: {total}")

generated = 0

for idx, key in enumerate(sorted_keys):
    bench, sf, nq = key

    sg = lk_sirius_gpu.get(key)
    sc = lk_sirius_cpu.get(key)
    mg = lk_maximus_gpu.get(key)
    mc = lk_maximus_cpu.get(key)

    sg_data = sg[0] if sg else []
    sc_data = sc[0] if sc else []
    mg_data = mg[0] if mg else []
    mc_data = mc[0] if mc else []

    if not any([sg_data, sc_data, mg_data, mc_data]):
        continue

    sg_nreps = sg[1] if sg else None
    sc_nreps = sc[1] if sc else None
    mg_nreps = mg[1] if mg else None
    mc_nreps = mc[1] if mc else None

    # Build reps string for filename
    reps_parts = []
    if sg_data:
        reps_parts.append(f"SG{sg_nreps}r" if sg_nreps else "SG-r")
    if sc_data:
        reps_parts.append(f"SC{sc_nreps}r" if sc_nreps else "SC-r")
    if mg_data:
        reps_parts.append(f"MG{mg_nreps}r" if mg_nreps else "MG-r")
    if mc_data:
        reps_parts.append(f"MC{mc_nreps}r" if mc_nreps else "MC-r")
    reps_str = "_".join(reps_parts)

    fname = f"{bench}_sf{sf}_{nq}_{reps_str}.png"
    outpath = OUT_DIR / fname

    fig, axes = plt.subplots(2, 2, figsize=(15, 9))
    fig.suptitle(
        f"{bench.upper()} SF={sf} {nq.upper()} — GPU & CPU Power Traces\n"
        f"Sirius: GPU-data {sg_nreps or 'N/A'}reps, CPU-data {sc_nreps or 'N/A'}reps | "
        f"Maximus: GPU-data {mg_nreps or 'N/A'}reps, CPU-data {mc_nreps or 'N/A'}reps",
        fontsize=12, fontweight="bold"
    )

    plot_panel(axes[0, 0], sg_data,
               f"Sirius — Data on GPU ({sg_nreps or 'N/A'} reps)")
    plot_panel(axes[0, 1], sc_data,
               f"Sirius — Data on CPU ({sc_nreps or 'N/A'} reps)")
    plot_panel(axes[1, 0], mg_data,
               f"Maximus — Data on GPU ({mg_nreps or 'N/A'} reps)")
    plot_panel(axes[1, 1], mc_data,
               f"Maximus — Data on CPU ({mc_nreps or 'N/A'} reps)")

    plt.tight_layout(rect=[0, 0, 1, 0.91])
    fig.savefig(outpath, dpi=120)
    plt.close(fig)
    generated += 1

    if (idx + 1) % 20 == 0 or idx == total - 1:
        print(f"  [{idx+1}/{total}] Generated {generated}")

print(f"\nDone! Generated {generated} PNGs")
print(f"Output: {OUT_DIR}")

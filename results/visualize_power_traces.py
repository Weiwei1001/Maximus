#!/usr/bin/env python3
"""
Visualize GPU and CPU power traces for Sirius and Maximus,
comparing data-on-GPU vs data-on-CPU scenarios.

Generates a 2x2 grid: (Sirius/Maximus) x (GPU-data/CPU-data)
showing GPU power, CPU power, and GPU utilization over time.
"""
import csv
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np
from pathlib import Path

RESULTS = Path("/home/xzw/gpu_db/results")

def load_samples(fpath, run_id_filter):
    """Load samples matching a run_id prefix."""
    rows = []
    with open(fpath) as f:
        for r in csv.DictReader(f):
            if r["run_id"] == run_id_filter:
                rows.append({
                    "t": float(r["time_offset_ms"]) / 1000.0,
                    "gpu_w": float(r["power_w"]),
                    "gpu_util": float(r["gpu_util_pct"]),
                    "mem_mb": float(r["mem_used_mb"]),
                    "cpu_w": float(r["cpu_pkg_power_w"]),
                })
    return rows


def plot_power_trace(ax, samples, title, color_gpu="tab:red", color_cpu="tab:blue"):
    """Plot GPU and CPU power + GPU utilization on dual y-axis."""
    if not samples:
        ax.text(0.5, 0.5, "No Data", transform=ax.transAxes, ha="center", va="center", fontsize=14)
        ax.set_title(title, fontsize=11, fontweight="bold")
        return

    t = [s["t"] for s in samples]
    gpu_w = [s["gpu_w"] for s in samples]
    cpu_w = [s["cpu_w"] for s in samples]
    gpu_util = [s["gpu_util"] for s in samples]

    # GPU power
    l1, = ax.plot(t, gpu_w, color=color_gpu, linewidth=1.5, label="GPU Power (W)")
    # CPU power
    l2, = ax.plot(t, cpu_w, color=color_cpu, linewidth=1.5, label="CPU Power (W)")

    ax.set_xlabel("Time (s)", fontsize=9)
    ax.set_ylabel("Power (W)", fontsize=9)
    ax.set_title(title, fontsize=11, fontweight="bold")

    # GPU utilization on right axis
    ax2 = ax.twinx()
    l3, = ax2.plot(t, gpu_util, color="tab:green", linewidth=1.2, linestyle="--", alpha=0.7, label="GPU Util (%)")
    ax2.set_ylabel("GPU Util (%)", fontsize=9, color="tab:green")
    ax2.set_ylim(0, 105)
    ax2.tick_params(axis="y", labelcolor="tab:green")

    lines = [l1, l2, l3]
    labels = [l.get_label() for l in lines]
    ax.legend(lines, labels, loc="upper right", fontsize=7)

    # Annotations
    avg_gpu = np.mean(gpu_w)
    avg_cpu = np.mean([c for c in cpu_w if c > 0])
    avg_util = np.mean(gpu_util)
    ax.axhline(avg_gpu, color=color_gpu, linestyle=":", alpha=0.4)
    ax.axhline(avg_cpu, color=color_cpu, linestyle=":", alpha=0.4)
    ax.text(0.02, 0.95, f"Avg GPU: {avg_gpu:.0f}W\nAvg CPU: {avg_cpu:.0f}W\nAvg Util: {avg_util:.0f}%",
            transform=ax.transAxes, fontsize=8, va="top",
            bbox=dict(boxstyle="round,pad=0.3", facecolor="wheat", alpha=0.8))


# ── Pick representative query: TPC-H Q9 at SF=1 ──
QUERY_SIRIUS = "q09"  # Sirius uses 2-digit
QUERY_MAXIMUS = "q9"  # Maximus uses 1-digit
SF = "1"
QUERY_LABEL = "TPC-H Q9, SF=1"

# ── Load all 4 scenarios ──

# 1) Sirius GPU-data
sirius_gpu = load_samples(
    RESULTS / "sirius_tpch_sf1_metrics_samples_20260301_025436.csv",
    f"tpch_sf{SF}_{QUERY_SIRIUS}"
)

# 2) Sirius CPU-data
sirius_cpu = load_samples(
    RESULTS / "sirius_cpu_data_samples_20260301_034912.csv",
    f"tpch_sf{SF}_cpu_{QUERY_SIRIUS}"
)

# 3) Maximus GPU-data
maximus_gpu = load_samples(
    RESULTS / "maximus_tpch_sf1_metrics_samples_20260301_021440.csv",
    f"tpch_sf{SF}_{QUERY_MAXIMUS}"
)

# 4) Maximus CPU-data
maximus_cpu = load_samples(
    RESULTS / "maximus_cpu_data_metrics_samples_20260301_061838.csv",
    f"tpch_sf{SF}_cpu_{QUERY_MAXIMUS}"
)

print(f"Loaded samples: Sirius GPU={len(sirius_gpu)}, Sirius CPU={len(sirius_cpu)}, "
      f"Maximus GPU={len(maximus_gpu)}, Maximus CPU={len(maximus_cpu)}")

# ── Plot 2x2 ──
fig, axes = plt.subplots(2, 2, figsize=(16, 10))
fig.suptitle(f"GPU & CPU Power Traces: {QUERY_LABEL}\nData-on-GPU vs Data-on-CPU", fontsize=14, fontweight="bold")

plot_power_trace(axes[0, 0], sirius_gpu,  "Sirius — Data on GPU")
plot_power_trace(axes[0, 1], sirius_cpu,  "Sirius — Data on CPU (first-run transfer)")
plot_power_trace(axes[1, 0], maximus_gpu, "Maximus — Data on GPU (-s gpu)")
plot_power_trace(axes[1, 1], maximus_cpu, "Maximus — Data on CPU (-s cpu)")

plt.tight_layout(rect=[0, 0, 1, 0.93])
out = RESULTS / "power_trace_q9_sf1.png"
fig.savefig(out, dpi=150)
print(f"Saved: {out}")

# ── Also create a bar chart summary for all TPC-H queries ──
fig2, axes2 = plt.subplots(2, 2, figsize=(18, 12))
fig2.suptitle("Average Power by Query: TPC-H SF=1\nGPU-data vs CPU-data", fontsize=14, fontweight="bold")

def make_bar_chart(ax, title, gpu_summary_file, cpu_summary_file_or_rows,
                   engine, gpu_run_prefix, cpu_run_prefix, gpu_samples_file, cpu_samples_file):
    """Create grouped bar chart of GPU/CPU power per query."""
    # Load GPU-data summary
    with open(gpu_summary_file) as f:
        gpu_summ = {r["query"]: r for r in csv.DictReader(f) if r["status"] == "OK" and r["sf"] == "1"}

    # Load CPU-data summary
    if isinstance(cpu_summary_file_or_rows, list):
        cpu_summ = {r["query"]: r for r in cpu_summary_file_or_rows
                    if r["status"] == "OK" and r.get("benchmark", "") == "tpch" and r["sf"] == "1"
                    and float(r.get("cpu_energy_j", r.get("avg_cpu_pkg_w", 0))) > 0}
    else:
        with open(cpu_summary_file_or_rows) as f:
            cpu_summ = {r["query"]: r for r in csv.DictReader(f)
                        if r["status"] == "OK" and r.get("benchmark", "") == "tpch" and str(r["sf"]) == "1"
                        and float(r.get("avg_cpu_pkg_w", 0)) > 0}

    queries = sorted(set(list(gpu_summ.keys()) + list(cpu_summ.keys())))
    if not queries:
        return

    x = np.arange(len(queries))
    w = 0.2

    gpu_data_gpu_w = []
    gpu_data_cpu_w = []
    cpu_data_gpu_w = []
    cpu_data_cpu_w = []

    for q in queries:
        if q in gpu_summ:
            gpu_data_gpu_w.append(float(gpu_summ[q]["avg_power_w"]))
            gpu_data_cpu_w.append(float(gpu_summ[q]["avg_cpu_pkg_w"]))
        else:
            gpu_data_gpu_w.append(0)
            gpu_data_cpu_w.append(0)

        if q in cpu_summ:
            r = cpu_summ[q]
            if engine == "sirius":
                gpu_w = float(r["gpu_power_avg_w"])
                cpu_e = float(r["cpu_energy_j"])
                wall = float(r["wall_time_s"])
                cpu_w = cpu_e / wall if wall > 0 else 0
            else:
                gpu_w = float(r["avg_power_w"])
                cpu_w = float(r["avg_cpu_pkg_w"])
            cpu_data_gpu_w.append(gpu_w)
            cpu_data_cpu_w.append(cpu_w)
        else:
            cpu_data_gpu_w.append(0)
            cpu_data_cpu_w.append(0)

    ax.bar(x - 1.5*w, gpu_data_gpu_w, w, label="GPU-data: GPU Power", color="tab:red", alpha=0.8)
    ax.bar(x - 0.5*w, gpu_data_cpu_w, w, label="GPU-data: CPU Power", color="tab:blue", alpha=0.8)
    ax.bar(x + 0.5*w, cpu_data_gpu_w, w, label="CPU-data: GPU Power", color="tab:orange", alpha=0.8)
    ax.bar(x + 1.5*w, cpu_data_cpu_w, w, label="CPU-data: CPU Power", color="tab:cyan", alpha=0.8)

    ax.set_xticks(x)
    ax.set_xticklabels(queries, rotation=45, ha="right", fontsize=8)
    ax.set_ylabel("Average Power (W)", fontsize=10)
    ax.set_title(title, fontsize=12, fontweight="bold")
    ax.legend(fontsize=7, ncol=2)
    ax.set_ylim(0, max(max(gpu_data_gpu_w + [1]), max(cpu_data_cpu_w + [1])) * 1.2)


# Load Sirius CPU-data analysis rows
with open(RESULTS / "sirius_cpu_data_analysis.csv") as f:
    sirius_cpu_rows = list(csv.DictReader(f))

# Sirius bar charts
make_bar_chart(axes2[0, 0], "Sirius — TPC-H SF=1: Power per Query",
               RESULTS / "sirius_tpch_sf1_metrics_summary_20260301_025436.csv",
               sirius_cpu_rows, "sirius",
               "tpch_sf1_", "tpch_sf1_cpu_",
               RESULTS / "sirius_tpch_sf1_metrics_samples_20260301_025436.csv",
               RESULTS / "sirius_cpu_data_samples_20260301_034912.csv")

# Maximus bar charts
make_bar_chart(axes2[1, 0], "Maximus — TPC-H SF=1: Power per Query",
               RESULTS / "maximus_tpch_sf1_metrics_summary_20260301_021440.csv",
               RESULTS / "maximus_cpu_data_metrics_summary_20260301_061838.csv",
               "maximus",
               "tpch_sf1_", "tpch_sf1_cpu_",
               RESULTS / "maximus_tpch_sf1_metrics_samples_20260301_021440.csv",
               RESULTS / "maximus_cpu_data_metrics_samples_20260301_061838.csv")

# Energy comparison (GPU-data vs CPU-data)
def make_energy_bar(ax, title, gpu_summary_file, cpu_rows_or_file, engine):
    """Energy per query comparison."""
    with open(gpu_summary_file) as f:
        gpu_summ = {r["query"]: r for r in csv.DictReader(f) if r["status"] == "OK" and r["sf"] == "1"}

    if isinstance(cpu_rows_or_file, list):
        cpu_summ = {r["query"]: r for r in cpu_rows_or_file
                    if r["status"] == "OK" and r.get("benchmark", "") == "tpch" and r["sf"] == "1"
                    and float(r.get("cpu_energy_j", 0)) > 0}
    else:
        with open(cpu_rows_or_file) as f:
            cpu_summ = {r["query"]: r for r in csv.DictReader(f)
                        if r["status"] == "OK" and r.get("benchmark", "") == "tpch" and str(r["sf"]) == "1"
                        and float(r.get("cpu_energy_j", 0)) > 0}

    queries = sorted(set(list(gpu_summ.keys()) + list(cpu_summ.keys())))
    if not queries:
        return

    x = np.arange(len(queries))
    w = 0.2

    gpu_gpu_e, gpu_cpu_e, cpu_gpu_e, cpu_cpu_e = [], [], [], []
    for q in queries:
        if q in gpu_summ:
            r = gpu_summ[q]
            gpu_gpu_e.append(float(r.get("energy_j", r.get("gpu_energy_j", 0))))
            gpu_cpu_e.append(float(r["cpu_energy_j"]))
        else:
            gpu_gpu_e.append(0)
            gpu_cpu_e.append(0)
        if q in cpu_summ:
            r = cpu_summ[q]
            cpu_gpu_e.append(float(r["gpu_energy_j"]))
            cpu_cpu_e.append(float(r["cpu_energy_j"]))
        else:
            cpu_gpu_e.append(0)
            cpu_cpu_e.append(0)

    ax.bar(x - 1.5*w, gpu_gpu_e, w, label="GPU-data: GPU Energy", color="tab:red", alpha=0.8)
    ax.bar(x - 0.5*w, gpu_cpu_e, w, label="GPU-data: CPU Energy", color="tab:blue", alpha=0.8)
    ax.bar(x + 0.5*w, cpu_gpu_e, w, label="CPU-data: GPU Energy", color="tab:orange", alpha=0.8)
    ax.bar(x + 1.5*w, cpu_cpu_e, w, label="CPU-data: CPU Energy", color="tab:cyan", alpha=0.8)

    ax.set_xticks(x)
    ax.set_xticklabels(queries, rotation=45, ha="right", fontsize=8)
    ax.set_ylabel("Energy (J)", fontsize=10)
    ax.set_title(title, fontsize=12, fontweight="bold")
    ax.legend(fontsize=7, ncol=2)

make_energy_bar(axes2[0, 1], "Sirius — TPC-H SF=1: Energy per Query",
                RESULTS / "sirius_tpch_sf1_metrics_summary_20260301_025436.csv",
                sirius_cpu_rows, "sirius")
make_energy_bar(axes2[1, 1], "Maximus — TPC-H SF=1: Energy per Query",
                RESULTS / "maximus_tpch_sf1_metrics_summary_20260301_021440.csv",
                RESULTS / "maximus_cpu_data_metrics_summary_20260301_061838.csv",
                "maximus")

plt.tight_layout(rect=[0, 0, 1, 0.93])
out2 = RESULTS / "power_energy_comparison_tpch_sf1.png"
fig2.savefig(out2, dpi=150)
print(f"Saved: {out2}")


# ── Figure 3: CPU energy scaling with data size ──
fig3, axes3 = plt.subplots(1, 2, figsize=(14, 6))
fig3.suptitle("CPU Energy Scaling with Data Size (Data-on-CPU)", fontsize=14, fontweight="bold")

# Sirius: CPU energy vs SF
sirius_sfs = []
sirius_cpu_energies = []
sirius_gpu_energies = []
for sf_val, sf_num in [("1", 1), ("2", 2), ("5", 5), ("10", 10)]:
    ok = [r for r in sirius_cpu_rows if r["benchmark"] == "tpch" and r["sf"] == sf_val
          and r["status"] == "OK" and float(r["cpu_energy_j"]) > 0]
    if ok:
        sirius_sfs.append(sf_num)
        sirius_cpu_energies.append(np.mean([float(r["cpu_energy_j"]) for r in ok]))
        sirius_gpu_energies.append(np.mean([float(r["gpu_energy_j"]) for r in ok]))

ax3 = axes3[0]
x = np.arange(len(sirius_sfs))
w = 0.35
ax3.bar(x - w/2, sirius_gpu_energies, w, label="GPU Energy (J)", color="tab:red", alpha=0.8)
ax3.bar(x + w/2, sirius_cpu_energies, w, label="CPU Energy (J)", color="tab:blue", alpha=0.8)
ax3.set_xticks(x)
ax3.set_xticklabels([f"SF={s}" for s in sirius_sfs])
ax3.set_ylabel("Avg Energy per Query (J)")
ax3.set_title("Sirius — TPC-H: CPU vs GPU Energy by Scale Factor")
ax3.legend()
for i, (g, c) in enumerate(zip(sirius_gpu_energies, sirius_cpu_energies)):
    ax3.text(i - w/2, g + 5, f"{g:.0f}J", ha="center", fontsize=8)
    ax3.text(i + w/2, c + 5, f"{c:.0f}J", ha="center", fontsize=8)

# Maximus: CPU energy vs SF
with open(RESULTS / "maximus_cpu_data_metrics_summary_20260301_061838.csv") as f:
    m_cpu_rows = [r for r in csv.DictReader(f) if r["status"] == "OK" and float(r.get("cpu_energy_j", 0)) > 0]

max_sfs = []
max_cpu_energies = []
max_gpu_energies = []
for sf_val, sf_num in [("1", 1), ("2", 2), ("5", 5), ("10", 10)]:
    ok = [r for r in m_cpu_rows if r["benchmark"] == "tpch" and str(r["sf"]) == sf_val]
    if ok:
        max_sfs.append(sf_num)
        max_cpu_energies.append(np.mean([float(r["cpu_energy_j"]) for r in ok]))
        max_gpu_energies.append(np.mean([float(r["gpu_energy_j"]) for r in ok]))

ax4 = axes3[1]
x2 = np.arange(len(max_sfs))
ax4.bar(x2 - w/2, max_gpu_energies, w, label="GPU Energy (J)", color="tab:red", alpha=0.8)
ax4.bar(x2 + w/2, max_cpu_energies, w, label="CPU Energy (J)", color="tab:blue", alpha=0.8)
ax4.set_xticks(x2)
ax4.set_xticklabels([f"SF={s}" for s in max_sfs])
ax4.set_ylabel("Avg Energy per Query (J)")
ax4.set_title("Maximus — TPC-H: CPU vs GPU Energy by Scale Factor")
ax4.legend()
for i, (g, c) in enumerate(zip(max_gpu_energies, max_cpu_energies)):
    ax4.text(i - w/2, g + 0.5, f"{g:.1f}J", ha="center", fontsize=8)
    ax4.text(i + w/2, c + 0.5, f"{c:.1f}J", ha="center", fontsize=8)

plt.tight_layout(rect=[0, 0, 1, 0.93])
out3 = RESULTS / "energy_scaling_tpch.png"
fig3.savefig(out3, dpi=150)
print(f"Saved: {out3}")


# ── Figure 4: Detailed single-query timeline (Q9 SF=10 for Sirius, large data) ──
fig4, axes4 = plt.subplots(1, 2, figsize=(16, 6))
fig4.suptitle("Power Timeline: TPC-H Q9 SF=10 — Data-on-CPU\n(Larger data = more visible transfer overhead)",
              fontsize=13, fontweight="bold")

# Sirius Q9 SF10
sirius_q9_sf10 = load_samples(
    RESULTS / "sirius_cpu_data_samples_20260301_034912.csv",
    "tpch_sf10_cpu_q09"
)
plot_power_trace(axes4[0], sirius_q9_sf10, "Sirius — Q9 SF=10 (Data on CPU)")

# Maximus Q9 SF10
maximus_q9_sf10 = load_samples(
    RESULTS / "maximus_cpu_data_metrics_samples_20260301_061838.csv",
    "tpch_sf10_cpu_q9"
)
plot_power_trace(axes4[1], maximus_q9_sf10, "Maximus — Q9 SF=10 (Data on CPU)")

plt.tight_layout(rect=[0, 0, 1, 0.90])
out4 = RESULTS / "power_trace_q9_sf10.png"
fig4.savefig(out4, dpi=150)
print(f"Saved: {out4}")

print("\nAll visualizations generated!")

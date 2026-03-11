#!/usr/bin/env python3
"""Plot frequency experiment results for all engines."""
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np

SAMPLES = "freq_all_samples_20260307_112304.csv"
SUMMARY = "freq_all_summary_20260307_112304.csv"

df = pd.read_csv(SAMPLES)
summary = pd.read_csv(SUMMARY)

configs = ["baseline", "cpu_low", "gpu_low", "both_low"]
engines = ["sirius_gpu", "maximus_gpu", "maximus_cpu"]
engine_labels = {"sirius_gpu": "Sirius GPU", "maximus_gpu": "Maximus GPU", "maximus_cpu": "Maximus CPU"}

cfg_colors = {
    "baseline": "#2196F3", "cpu_low": "#FF9800",
    "gpu_low": "#4CAF50", "both_low": "#F44336",
}
cfg_labels = {
    "baseline": "Baseline\n(CPU=4.4G, GPU=auto)",
    "cpu_low":  "CPU Low\n(CPU=0.8G, GPU=auto)",
    "gpu_low":  "GPU Low\n(CPU=4.4G, GPU=180M)",
    "both_low": "Both Low\n(CPU=0.8G, GPU=180M)",
}

# ── Figure 1: Power traces per engine (3 rows × 2 cols) ──────────────────────
fig1, axes = plt.subplots(3, 2, figsize=(18, 14))
fig1.suptitle("Power Traces — TPC-H SF=5 Q1 (All Engines)", fontsize=16, fontweight="bold")

for row, eng in enumerate(engines):
    ax_gpu = axes[row, 0]
    ax_cpu = axes[row, 1]
    for cfg in configs:
        d = df[(df["config"] == cfg) & (df["engine"] == eng)]
        if len(d) == 0:
            continue
        t = d["time_offset_ms"].values / 1000.0
        ax_gpu.plot(t, d["gpu_power_w"], color=cfg_colors[cfg], alpha=0.8, lw=1.2, label=cfg)
        ax_cpu.plot(t, d["cpu_pkg_w"], color=cfg_colors[cfg], alpha=0.8, lw=1.2, label=cfg)

    ax_gpu.set_title(f"{engine_labels[eng]} — GPU Power")
    ax_gpu.set_ylabel("GPU Power (W)")
    ax_gpu.legend(fontsize=8)
    ax_gpu.grid(True, alpha=0.3)

    ax_cpu.set_title(f"{engine_labels[eng]} — CPU Power")
    ax_cpu.set_ylabel("CPU Package Power (W)")
    ax_cpu.legend(fontsize=8)
    ax_cpu.grid(True, alpha=0.3)

    if row == 2:
        ax_gpu.set_xlabel("Time (s)")
        ax_cpu.set_xlabel("Time (s)")

fig1.tight_layout(rect=[0, 0, 1, 0.96])
fig1.savefig("freq_all_power_traces.png", dpi=150, bbox_inches="tight")
print("Saved: freq_all_power_traces.png")
plt.close(fig1)

# ── Figure 2: Bar charts comparison ──────────────────────────────────────────
fig2 = plt.figure(figsize=(18, 10))
gs = gridspec.GridSpec(2, 2, hspace=0.4, wspace=0.3, top=0.92, bottom=0.08)
fig2.suptitle("Frequency Scaling — TPC-H SF=5 Q1 Comparison", fontsize=16, fontweight="bold")

x = np.arange(len(configs))
width = 0.25

# Plot 1: Query latency grouped by engine
ax1 = fig2.add_subplot(gs[0, 0])
for i, eng in enumerate(engines):
    vals = []
    for cfg in configs:
        row = summary[(summary["config"] == cfg) & (summary["engine"] == eng)]
        vals.append(float(row["min_s"].values[0]) if len(row) > 0 and row["status"].values[0] == "OK" else 0)
    bars = ax1.bar(x + i * width, vals, width, label=engine_labels[eng], alpha=0.85)
    for bar, v in zip(bars, vals):
        if v > 0:
            ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height(),
                     f"{v:.3f}", ha="center", va="bottom", fontsize=7, fontweight="bold")
ax1.set_xticks(x + width)
ax1.set_xticklabels([cfg_labels[c] for c in configs], fontsize=7)
ax1.set_ylabel("Query Latency (s)")
ax1.set_title("Min Query Time (lower = better)")
ax1.legend(fontsize=9)
ax1.grid(True, alpha=0.3, axis="y")

# Plot 2: GPU power
ax2 = fig2.add_subplot(gs[0, 1])
for i, eng in enumerate(engines):
    vals = []
    for cfg in configs:
        row = summary[(summary["config"] == cfg) & (summary["engine"] == eng)]
        vals.append(float(row["avg_gpu_w"].values[0]) if len(row) > 0 and row["status"].values[0] == "OK" else 0)
    bars = ax2.bar(x + i * width, vals, width, label=engine_labels[eng], alpha=0.85)
    for bar, v in zip(bars, vals):
        if v > 0:
            ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height(),
                     f"{v:.0f}W", ha="center", va="bottom", fontsize=7, fontweight="bold")
ax2.set_xticks(x + width)
ax2.set_xticklabels([cfg_labels[c] for c in configs], fontsize=7)
ax2.set_ylabel("Avg GPU Power (W)")
ax2.set_title("Steady-State GPU Power")
ax2.legend(fontsize=9)
ax2.grid(True, alpha=0.3, axis="y")

# Plot 3: GPU energy per query
ax3 = fig2.add_subplot(gs[1, 0])
for i, eng in enumerate(engines):
    vals = []
    for cfg in configs:
        row = summary[(summary["config"] == cfg) & (summary["engine"] == eng)]
        vals.append(float(row["gpu_energy_j"].values[0]) if len(row) > 0 and row["status"].values[0] == "OK" else 0)
    bars = ax3.bar(x + i * width, vals, width, label=engine_labels[eng], alpha=0.85)
    for bar, v in zip(bars, vals):
        if v > 0:
            ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height(),
                     f"{v:.1f}", ha="center", va="bottom", fontsize=7, fontweight="bold")
ax3.set_xticks(x + width)
ax3.set_xticklabels([cfg_labels[c] for c in configs], fontsize=7)
ax3.set_ylabel("GPU Energy per Query (J)")
ax3.set_title("GPU Energy (lower = better)")
ax3.legend(fontsize=9)
ax3.grid(True, alpha=0.3, axis="y")

# Plot 4: Total energy (GPU + CPU)
ax4 = fig2.add_subplot(gs[1, 1])
for i, eng in enumerate(engines):
    gpu_vals = []
    cpu_vals = []
    for cfg in configs:
        row = summary[(summary["config"] == cfg) & (summary["engine"] == eng)]
        if len(row) > 0 and row["status"].values[0] == "OK":
            gpu_vals.append(float(row["gpu_energy_j"].values[0]))
            cpu_vals.append(float(row["cpu_energy_j"].values[0]))
        else:
            gpu_vals.append(0)
            cpu_vals.append(0)
    total = [g + c for g, c in zip(gpu_vals, cpu_vals)]
    bars_g = ax4.bar(x + i * width, gpu_vals, width, label=f"{engine_labels[eng]} GPU" if i == 0 else "", alpha=0.85)
    bars_c = ax4.bar(x + i * width, cpu_vals, width, bottom=gpu_vals, alpha=0.4)
    for j, (bar, t) in enumerate(zip(bars_g, total)):
        if t > 0:
            ax4.text(bar.get_x() + bar.get_width()/2, t,
                     f"{t:.0f}", ha="center", va="bottom", fontsize=6, fontweight="bold")
ax4.set_xticks(x + width)
ax4.set_xticklabels([cfg_labels[c] for c in configs], fontsize=7)
ax4.set_ylabel("Total Energy per Query (J)")
ax4.set_title("Total Energy GPU+CPU (solid=GPU, faded=CPU)")
# Custom legend
from matplotlib.patches import Patch
legend_elements = [Patch(facecolor='tab:blue', alpha=0.85, label='Sirius GPU'),
                   Patch(facecolor='tab:orange', alpha=0.85, label='Maximus GPU'),
                   Patch(facecolor='tab:green', alpha=0.85, label='Maximus CPU'),
                   Patch(facecolor='gray', alpha=0.4, label='CPU portion')]
ax4.legend(handles=legend_elements, fontsize=8)
ax4.grid(True, alpha=0.3, axis="y")

fig2.savefig("freq_all_comparison.png", dpi=150, bbox_inches="tight")
print("Saved: freq_all_comparison.png")
plt.close(fig2)

# ── Figure 3: GPU utilization traces ─────────────────────────────────────────
fig3, axes3 = plt.subplots(3, 2, figsize=(18, 14))
fig3.suptitle("GPU Utilization & SM Clock — TPC-H SF=5 Q1", fontsize=16, fontweight="bold")

for row, eng in enumerate(engines):
    ax_util = axes3[row, 0]
    ax_clk = axes3[row, 1]
    for cfg in configs:
        d = df[(df["config"] == cfg) & (df["engine"] == eng)]
        if len(d) == 0:
            continue
        t = d["time_offset_ms"].values / 1000.0
        ax_util.plot(t, d["gpu_util_pct"], color=cfg_colors[cfg], alpha=0.8, lw=1.2, label=cfg)
        ax_clk.plot(t, d["sm_clk_mhz"], color=cfg_colors[cfg], alpha=0.8, lw=1.2, label=cfg)

    ax_util.set_title(f"{engine_labels[eng]} — GPU Utilization")
    ax_util.set_ylabel("GPU Util (%)")
    ax_util.set_ylim(-5, 105)
    ax_util.legend(fontsize=8)
    ax_util.grid(True, alpha=0.3)

    ax_clk.set_title(f"{engine_labels[eng]} — SM Clock")
    ax_clk.set_ylabel("SM Clock (MHz)")
    ax_clk.legend(fontsize=8)
    ax_clk.grid(True, alpha=0.3)

    if row == 2:
        ax_util.set_xlabel("Time (s)")
        ax_clk.set_xlabel("Time (s)")

fig3.tight_layout(rect=[0, 0, 1, 0.96])
fig3.savefig("freq_all_util_traces.png", dpi=150, bbox_inches="tight")
print("Saved: freq_all_util_traces.png")
plt.close(fig3)

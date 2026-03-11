#!/usr/bin/env python3
"""Plot frequency experiment v2 results (with real CPU freq control)."""
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np
from matplotlib.patches import Patch

SAMPLES = "freq_all_samples_20260307_113853.csv"
SUMMARY = "freq_all_summary_20260307_113853.csv"

df = pd.read_csv(SAMPLES)
summary = pd.read_csv(SUMMARY)

configs = ["baseline", "cpu_low", "gpu_low", "both_low"]
engines = ["sirius_gpu", "maximus_gpu", "maximus_cpu"]
engine_labels = {"sirius_gpu": "Sirius GPU", "maximus_gpu": "Maximus GPU", "maximus_cpu": "Maximus CPU"}
engine_colors = {"sirius_gpu": "#2196F3", "maximus_gpu": "#FF9800", "maximus_cpu": "#4CAF50"}

cfg_colors = {
    "baseline": "#2196F3", "cpu_low": "#FF9800",
    "gpu_low": "#4CAF50", "both_low": "#F44336",
}
cfg_short = {
    "baseline": "Baseline\n100%/auto",
    "cpu_low":  "CPU Low\n18%/auto",
    "gpu_low":  "GPU Low\n100%/180M",
    "both_low": "Both Low\n18%/180M",
}

def get_val(cfg, eng, col):
    row = summary[(summary["config"] == cfg) & (summary["engine"] == eng)]
    if len(row) > 0 and row["status"].values[0] == "OK":
        return float(row[col].values[0])
    return 0

# ── Figure 1: Power traces (3 engines × 2 cols) ─────────────────────────────
fig1, axes = plt.subplots(3, 2, figsize=(18, 14))
fig1.suptitle("Power Traces — TPC-H SF=5 Q1 (CPU freq fix applied)", fontsize=16, fontweight="bold")

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
fig1.savefig("freq_v2_power_traces.png", dpi=150, bbox_inches="tight")
print("Saved: freq_v2_power_traces.png")
plt.close(fig1)

# ── Figure 2: Bar comparison (2×2) ───────────────────────────────────────────
fig2 = plt.figure(figsize=(18, 12))
gs = gridspec.GridSpec(2, 2, hspace=0.4, wspace=0.3, top=0.92, bottom=0.08)
fig2.suptitle("Frequency Scaling v2 — TPC-H SF=5 Q1 (real CPU control)", fontsize=16, fontweight="bold")

x = np.arange(len(configs))
width = 0.25

# (0,0) Query latency
ax1 = fig2.add_subplot(gs[0, 0])
for i, eng in enumerate(engines):
    vals = [get_val(c, eng, "min_s") for c in configs]
    bars = ax1.bar(x + i*width, vals, width, label=engine_labels[eng],
                   color=engine_colors[eng], alpha=0.85)
    for bar, v in zip(bars, vals):
        if v > 0:
            ax1.text(bar.get_x()+bar.get_width()/2, bar.get_height(),
                     f"{v:.3f}", ha="center", va="bottom", fontsize=7, fontweight="bold")
ax1.set_xticks(x + width)
ax1.set_xticklabels([cfg_short[c] for c in configs], fontsize=8)
ax1.set_ylabel("Query Latency (s)")
ax1.set_title("Min Query Time (lower = better)")
ax1.legend(fontsize=9)
ax1.grid(True, alpha=0.3, axis="y")

# (0,1) Avg power (GPU + CPU side by side)
ax2 = fig2.add_subplot(gs[0, 1])
bar_w = 0.15
for i, eng in enumerate(engines):
    gpu_w = [get_val(c, eng, "avg_gpu_w") for c in configs]
    cpu_w = [get_val(c, eng, "avg_cpu_w") for c in configs]
    ax2.bar(x + i*2*bar_w, gpu_w, bar_w, color=engine_colors[eng], alpha=0.9,
            label=f"{engine_labels[eng]} GPU" if i == 0 else "")
    ax2.bar(x + i*2*bar_w + bar_w, cpu_w, bar_w, color=engine_colors[eng], alpha=0.35)
    for j, (g, c) in enumerate(zip(gpu_w, cpu_w)):
        ax2.text(x[j] + i*2*bar_w, g, f"{g:.0f}", ha="center", va="bottom", fontsize=6)
        ax2.text(x[j] + i*2*bar_w + bar_w, c, f"{c:.0f}", ha="center", va="bottom", fontsize=6)
ax2.set_xticks(x + 2.5*bar_w)
ax2.set_xticklabels([cfg_short[c] for c in configs], fontsize=8)
ax2.set_ylabel("Power (W)")
ax2.set_title("Steady-State Power (solid=GPU, faded=CPU)")
legend_pwr = [Patch(facecolor=engine_colors[e], alpha=0.9, label=f"{engine_labels[e]} GPU") for e in engines]
legend_pwr.append(Patch(facecolor="gray", alpha=0.35, label="CPU portion"))
ax2.legend(handles=legend_pwr, fontsize=7)
ax2.grid(True, alpha=0.3, axis="y")

# (1,0) GPU energy
ax3 = fig2.add_subplot(gs[1, 0])
for i, eng in enumerate(engines):
    vals = [get_val(c, eng, "gpu_energy_j") for c in configs]
    bars = ax3.bar(x + i*width, vals, width, label=engine_labels[eng],
                   color=engine_colors[eng], alpha=0.85)
    for bar, v in zip(bars, vals):
        if v > 0:
            ax3.text(bar.get_x()+bar.get_width()/2, bar.get_height(),
                     f"{v:.1f}", ha="center", va="bottom", fontsize=7, fontweight="bold")
ax3.set_xticks(x + width)
ax3.set_xticklabels([cfg_short[c] for c in configs], fontsize=8)
ax3.set_ylabel("GPU Energy per Query (J)")
ax3.set_title("GPU Energy (lower = better)")
ax3.legend(fontsize=9)
ax3.grid(True, alpha=0.3, axis="y")

# (1,1) Total energy (GPU + CPU stacked)
ax4 = fig2.add_subplot(gs[1, 1])
for i, eng in enumerate(engines):
    gpu_e = [get_val(c, eng, "gpu_energy_j") for c in configs]
    cpu_e = [get_val(c, eng, "cpu_energy_j") for c in configs]
    total = [g + c for g, c in zip(gpu_e, cpu_e)]
    ax4.bar(x + i*width, gpu_e, width, color=engine_colors[eng], alpha=0.85,
            label=engine_labels[eng] if i < 3 else "")
    ax4.bar(x + i*width, cpu_e, width, bottom=gpu_e, color=engine_colors[eng], alpha=0.3)
    for j, t in enumerate(total):
        if t > 0:
            ax4.text(x[j] + i*width, t, f"{t:.0f}", ha="center", va="bottom", fontsize=6, fontweight="bold")
ax4.set_xticks(x + width)
ax4.set_xticklabels([cfg_short[c] for c in configs], fontsize=8)
ax4.set_ylabel("Total Energy per Query (J)")
ax4.set_title("Total Energy GPU+CPU (solid=GPU, faded=CPU)")
legend_e = [Patch(facecolor=engine_colors[e], alpha=0.85, label=engine_labels[e]) for e in engines]
legend_e.append(Patch(facecolor="gray", alpha=0.3, label="CPU portion"))
ax4.legend(handles=legend_e, fontsize=8)
ax4.grid(True, alpha=0.3, axis="y")

fig2.savefig("freq_v2_comparison.png", dpi=150, bbox_inches="tight")
print("Saved: freq_v2_comparison.png")
plt.close(fig2)

# ── Figure 3: Speedup / slowdown heatmap ─────────────────────────────────────
fig3, (ax_time, ax_energy) = plt.subplots(1, 2, figsize=(16, 5))
fig3.suptitle("Relative Change vs Baseline — TPC-H SF=5 Q1", fontsize=14, fontweight="bold")

# Time ratio
time_data = []
for eng in engines:
    base = get_val("baseline", eng, "min_s")
    row = []
    for cfg in configs:
        v = get_val(cfg, eng, "min_s")
        row.append(v / base if base > 0 else 0)
    time_data.append(row)
time_arr = np.array(time_data)

im1 = ax_time.imshow(time_arr, cmap="RdYlGn_r", aspect="auto", vmin=0.5, vmax=10)
for i in range(len(engines)):
    for j in range(len(configs)):
        ax_time.text(j, i, f"{time_arr[i,j]:.2f}x", ha="center", va="center",
                     fontsize=11, fontweight="bold",
                     color="white" if time_arr[i,j] > 3 else "black")
ax_time.set_xticks(range(len(configs)))
ax_time.set_xticklabels(configs, fontsize=9)
ax_time.set_yticks(range(len(engines)))
ax_time.set_yticklabels([engine_labels[e] for e in engines], fontsize=9)
ax_time.set_title("Latency Ratio (1.0 = same as baseline)")
plt.colorbar(im1, ax=ax_time, shrink=0.8)

# Total energy ratio
energy_data = []
for eng in engines:
    base_g = get_val("baseline", eng, "gpu_energy_j")
    base_c = get_val("baseline", eng, "cpu_energy_j")
    base = base_g + base_c
    row = []
    for cfg in configs:
        g = get_val(cfg, eng, "gpu_energy_j")
        c = get_val(cfg, eng, "cpu_energy_j")
        row.append((g + c) / base if base > 0 else 0)
    energy_data.append(row)
energy_arr = np.array(energy_data)

im2 = ax_energy.imshow(energy_arr, cmap="RdYlGn_r", aspect="auto", vmin=0.3, vmax=10)
for i in range(len(engines)):
    for j in range(len(configs)):
        ax_energy.text(j, i, f"{energy_arr[i,j]:.2f}x", ha="center", va="center",
                       fontsize=11, fontweight="bold",
                       color="white" if energy_arr[i,j] > 3 else "black")
ax_energy.set_xticks(range(len(configs)))
ax_energy.set_xticklabels(configs, fontsize=9)
ax_energy.set_yticks(range(len(engines)))
ax_energy.set_yticklabels([engine_labels[e] for e in engines], fontsize=9)
ax_energy.set_title("Total Energy Ratio (1.0 = same as baseline)")
plt.colorbar(im2, ax=ax_energy, shrink=0.8)

fig3.tight_layout(rect=[0, 0, 1, 0.93])
fig3.savefig("freq_v2_heatmap.png", dpi=150, bbox_inches="tight")
print("Saved: freq_v2_heatmap.png")
plt.close(fig3)

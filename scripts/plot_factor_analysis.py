#!/usr/bin/env python3
"""Generate 4-layer energy factor analysis diagram. Arrows point upward TO Energy."""

import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch


def draw_box(ax, cx, cy, text, color, w=1.6, h=0.45, fontsize=8,
             bold=False, edgecolor='#444444', lw=1.0):
    box = FancyBboxPatch(
        (cx - w/2, cy - h/2), w, h,
        boxstyle="round,pad=0.06",
        facecolor=color, edgecolor=edgecolor, linewidth=lw,
    )
    ax.add_patch(box)
    weight = 'bold' if bold else 'normal'
    ax.text(cx, cy, text, ha='center', va='center', fontsize=fontsize,
            fontweight=weight, fontfamily='sans-serif')


def arrow(ax, x1, y1, x2, y2, color='#555', lw=1.0, dashed=False):
    ls = (0, (4, 3)) if dashed else '-'
    ax.annotate('', xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(arrowstyle='->', color=color, lw=lw,
                                linestyle=ls, shrinkA=4, shrinkB=4))


def group_box(ax, x, y, w, h, label, color, lw=1.0):
    rect = FancyBboxPatch(
        (x, y), w, h, boxstyle="round,pad=0.1",
        facecolor=color, edgecolor='#aaa', linewidth=lw,
        linestyle='--', alpha=0.3,
    )
    ax.add_patch(rect)
    ax.text(x + w/2, y + h - 0.06, label, ha='center', va='top',
            fontsize=7.5, fontweight='bold', color='#555', fontstyle='italic')


def main():
    fig, ax = plt.subplots(1, 1, figsize=(14, 13))
    ax.set_xlim(-0.5, 14.5)
    ax.set_ylim(-0.5, 14.0)
    ax.set_aspect('equal')
    ax.axis('off')

    # Colors
    C_EN = '#FFD700'
    C_L2 = '#FFF8DC'
    C_KN = '#FADBD8'
    C_HW = '#DAEAF6'
    C_WK = '#E8DAEF'

    E_BLK = '#333333'
    E_KN = '#C0392B'
    E_HW = '#2E86C1'
    E_WK = '#7B1FA2'

    # ═══════════════ LAYER 1: ENERGY (top) ═══════════════
    y1 = 12.5
    draw_box(ax, 7.0, y1, 'Energy (J)', C_EN, w=2.6, h=0.65,
             fontsize=14, bold=True, edgecolor='#B8860B', lw=2.5)

    # ═══════════════ LAYER 2: SWITCHING POWER, LATENCY, STATIC POWER ═══════════════
    y2 = 10.5
    draw_box(ax, 2.5, y2, 'Switching\nPower', C_L2, w=2.0, h=0.6, fontsize=10, bold=True)
    draw_box(ax, 7.0, y2, 'Latency', C_L2, w=2.0, h=0.6, fontsize=10, bold=True)
    draw_box(ax, 11.5, y2, 'Static\nPower', C_L2, w=2.0, h=0.6, fontsize=10, bold=True)

    # L2 → Energy (arrows point UP to Energy)
    arrow(ax, 2.5, y2+0.30, 7.0, y1-0.33, color=E_BLK, lw=2.0)
    arrow(ax, 7.0, y2+0.30, 7.0, y1-0.33, color=E_BLK, lw=2.0)
    arrow(ax, 11.5, y2+0.30, 7.0, y1-0.33, color=E_BLK, lw=2.0)

    # ═══════════════ LAYER 3: KNOBS + HW THROUGHPUT ═══════════════
    y3 = 7.8
    # Knob group box coords
    knob_x, knob_y, knob_w, knob_h = 0.2, y3-0.5, 6.0, 1.2
    group_box(ax, knob_x, knob_y, knob_w, knob_h,
              'Knob-Level (controllable)', '#E74C3C')
    knobs = [
        (1.2, y3, 'GPU Power\nLimit'),
        (3.2, y3, 'GPU\nFrequency'),
        (5.2, y3, 'SM Clock\nFrequency'),
    ]
    for cx, cy, t in knobs:
        draw_box(ax, cx, cy, t, C_KN, w=1.6, h=0.55, fontsize=8)

    # HW throughput group box coords
    hw_x, hw_y, hw_w, hw_h = 6.8, y3-0.5, 7.0, 1.2
    group_box(ax, hw_x, hw_y, hw_w, hw_h,
              'Hardware: Throughput Ceilings', '#85C1E9')
    hw = [
        (7.8, y3, 'GPU Memory\nBandwidth'),
        (9.9, y3, 'Interconnect\nBandwidth'),
        (12.1, y3, 'SM Count\n(CUDA Cores)'),
    ]
    for cx, cy, t in hw:
        draw_box(ax, cx, cy, t, C_HW, w=1.7, h=0.55, fontsize=8)

    # Knob block → Switching Power, Latency (color = target = L2 derived)
    knob_top_cx = knob_x + knob_w / 2
    knob_top_y = knob_y + knob_h
    arrow(ax, knob_top_cx - 0.5, knob_top_y, 2.5, y2-0.30, color=E_BLK, lw=1.5)
    arrow(ax, knob_top_cx + 0.5, knob_top_y, 7.0, y2-0.30, color=E_BLK, lw=1.5)

    # HW block → Latency, Static Power (color = target = L2 derived)
    hw_top_cx = hw_x + hw_w / 2
    hw_top_y = hw_y + hw_h
    arrow(ax, hw_top_cx - 0.5, hw_top_y, 7.0, y2-0.30, color=E_BLK, lw=1.5)
    arrow(ax, hw_top_cx + 0.5, hw_top_y, 11.5, y2-0.30, color=E_BLK, lw=1.5)

    # ═══════════════ LAYER 4: WORKLOAD TYPE ═══════════════
    y4 = 5.0
    group_box(ax, 0.2, y4-0.5, 13.6, 1.2, 'Workload-Level', '#8E44AD')
    wk = [
        (1.5, y4, 'Storage\nFormat'),
        (3.8, y4, 'Compute-\nHeavy'),
        (6.1, y4, 'Join-\nHeavy'),
        (8.4, y4, 'Scan-\nHeavy'),
        (11.0, y4, 'Small\nData'),
    ]
    for cx, cy, t in wk:
        draw_box(ax, cx, cy, t, C_WK, w=1.6, h=0.55, fontsize=8)

    # GPU Power Limit → GPU Frequency (within knob block)
    arrow(ax, 1.2, y3, 2.4, y3, color=E_KN, lw=1.0, dashed=True)

    # Compute-Heavy → SM Clock Freq (knob=red), GPU Memory BW (hw=blue)
    arrow(ax, 3.8, y4+0.28, 5.2, y3-0.28, color=E_KN, lw=1.0)    # SM Clock
    arrow(ax, 3.8, y4+0.28, 7.8, y3-0.28, color=E_HW, lw=1.0)    # GPU Mem BW

    # Join-Heavy → SM Clock (knob), GPU Memory BW (hw), Interconnect BW (hw)
    arrow(ax, 6.1, y4+0.28, 5.2, y3-0.28, color=E_KN, lw=1.0)    # SM Clock
    arrow(ax, 6.1, y4+0.28, 7.8, y3-0.28, color=E_HW, lw=1.0)    # GPU Mem BW
    arrow(ax, 6.1, y4+0.28, 9.9, y3-0.28, color=E_HW, lw=1.0)    # Interconnect

    # Scan-Heavy → GPU Memory BW (hw), Interconnect BW (hw)
    arrow(ax, 8.4, y4+0.28, 7.8, y3-0.28, color=E_HW, lw=1.0)    # GPU Mem BW
    arrow(ax, 8.4, y4+0.28, 9.9, y3-0.28, color=E_HW, lw=1.0)    # Interconnect

    # Small Data → GPU Frequency (knob=red), Interconnect BW (hw=blue)
    arrow(ax, 11.0, y4+0.28, 3.2, y3-0.28, color=E_KN, lw=1.0)   # GPU Freq
    arrow(ax, 11.0, y4+0.28, 9.9, y3-0.28, color=E_HW, lw=1.0)   # Interconnect

    # ═══════════════ LEGEND ═══════════════
    ly = 3.2
    items = [
        (C_HW, 'Hardware', E_HW),
        (C_KN, 'Knob', E_KN),
        (C_WK, 'Workload', E_WK),
        (C_L2, 'Derived', '#333'),
    ]
    for i, (c, lab, ec) in enumerate(items):
        x = 2.0 + i * 2.8
        box = FancyBboxPatch((x, ly), 0.3, 0.22, boxstyle="round,pad=0.02",
                              facecolor=c, edgecolor='#444', linewidth=0.7)
        ax.add_patch(box)
        ax.text(x + 0.45, ly + 0.11, lab, ha='left', va='center',
                fontsize=8, color=ec, fontweight='bold')

    # Title
    ax.text(7.0, 13.5,
            'Energy Factor Analysis: GPU-Accelerated SQL Engines',
            ha='center', fontsize=15, fontweight='bold', fontfamily='sans-serif')

    plt.tight_layout()
    out = '/home/xzw/gpu_db/results/factor_analysis_energy'
    fig.savefig(f'{out}.png', dpi=150, bbox_inches='tight', facecolor='white')
    fig.savefig(f'{out}.pdf', bbox_inches='tight', facecolor='white')
    print(f"Saved to {out}.png and {out}.pdf")
    plt.close()


if __name__ == '__main__':
    main()

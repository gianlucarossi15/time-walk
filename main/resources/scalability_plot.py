
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.ticker as mticker
import os

# ============================================================
# Font sizes (MATCHED to selectivity plots)
# ============================================================
SUPTITLE_SIZE = 40
RELATION_TITLE_SIZE = 35
AXIS_LABEL_SIZE = 35
TICK_LABEL_SIZE = 32
LEGEND_SIZE = 29
EMPTY_TEXT_SIZE = 20

# ============================================================
# Load data
# ============================================================
file = "main/resources/scalability_time.csv"
df = pd.read_csv(file, sep=";")

print("Loaded:", file)
print("Shape:", df.shape)
print("Columns:", df.columns.tolist())

# ============================================================
# Normalize columns
# ============================================================
for c in ['dataset', 'relation', 'ts']:
    df[c] = df[c].astype(str).str.strip()

df['window'] = pd.to_numeric(df['window'], errors='coerce')
df['query_time'] = pd.to_numeric(df['query_time'], errors='coerce')

# ms -> seconds
df['query_time'] = df['query_time'] / 1000.0

# ============================================================
# Split dataset family and numeric size
# ============================================================
# 'dataset' holds the family name (e.g. "finbench", "synthea").
# The numeric size lives in its own 'datasetSize' column, so use
# that directly instead of trying to parse it out of 'dataset'
# (which was the source of the AttributeError: 'float' object
# has no attribute 'endswith').
df['dataset_family'] = df['dataset'].str.split('_').str[0]

df['dataset_size_str'] = df['datasetSize'].astype(str).str.strip()


def parse_size(s):
    if not isinstance(s, str):
        return np.nan
    s = s.strip().lower()
    if s.endswith('k'):
        return int(s[:-1]) * 1_000
    if s.endswith('m'):
        return int(s[:-1]) * 1_000_000
    try:
        return int(s)
    except ValueError:
        return np.nan


df['dataset_size'] = df['dataset_size_str'].apply(parse_size)

# ============================================================
# Plot configuration
# ============================================================
relations = ['overlaps', 'before', 'meets', 'equal']
dataset_families = ['finbench']

# Colors (MATCHED to selectivity plots)
colors = {
    "overlaps": {"light": "#FFD580", "medium": "#FFA500", "dark": "#CC5500"},
    "meets":    {"light": "#ADD8E6", "medium": "#0000FF", "dark": "#00008B"},
    "before":   {"light": "#90EE90", "medium": "#008000", "dark": "#006400"},
    "equal":    {"light": "#D8BFD8", "medium": "#800080", "dark": "#4B0082"}
}


def apply_consistent_sci_notation(ax, max_ticks=5):
    """Force the same scientific/offset notation on every relation subplot's
    log-scaled y-axis, matching the clean look of the Synthea reference plot.

    Two things go wrong if you just set a ScalarFormatter on the default
    log-scale locators:
      1. Formatter-only fix (major formatter set, minor left alone): ticks
         landing exactly on a decade get the clean format, but any
         auto-inserted "minor" ticks (needed when a relation's values span
         less than one decade, e.g. overlaps/meets/equal here) fall back to
         the default LogFormatterSciNotation -> mixed "N x 10^k" / "1.00"
         styles within the same subplot.
      2. Naively also formatting ALL minor ticks fixes the style but not the
         clutter: log-scale minor ticks include every sub-decade value
         (2,3,4,5,6,7,8,9), and labeling all of them on a narrow range packs
         labels on top of each other.

    The robust fix is to take full manual control of *which* ticks are
    shown: build "nice" 1/2/5-per-decade candidates covering the visible
    range, thin them down to at most `max_ticks` if still dense, and turn
    off minor tick labels entirely. Every relation then gets a small,
    evenly log-spaced set of ticks in one consistent style, regardless of
    whether its value range happens to straddle a power of ten.
    """
    formatter = mticker.ScalarFormatter(useOffset=True, useMathText=False)
    formatter.set_scientific(True)
    formatter.set_powerlimits((0, 0))

    ymin, ymax = ax.get_ylim()
    lo = int(np.floor(np.log10(ymin))) - 1
    hi = int(np.ceil(np.log10(ymax))) + 1
    candidates = sorted(m * 10.0 ** e for e in range(lo, hi) for m in (1, 2, 5))
    ticks = [t for t in candidates if ymin <= t <= ymax]

    if len(ticks) > max_ticks:
        idx = np.linspace(0, len(ticks) - 1, max_ticks).round().astype(int)
        ticks = sorted(set(np.array(ticks)[idx]))

    ax.yaxis.set_major_locator(mticker.FixedLocator(ticks))
    ax.yaxis.set_minor_locator(mticker.NullLocator())
    ax.yaxis.set_major_formatter(formatter)
    ax.yaxis.get_offset_text().set_fontsize(25)


# ============================================================
# One figure per dataset family
# ============================================================
for family in dataset_families:

    df_family = df[df['dataset_family'] == family]

    if df_family.empty:
        print(f"No data for {family}, skipping.")
        continue

    print(f"\nCreating plot for {family}")

    windows = sorted(df_family['window'].unique())

    # NOTE: 'dataset' is just the family name (e.g. "synthea") and is the
    # SAME for every row/size in this CSV — the size only lives in
    # 'datasetSize'/'dataset_size_str'. So grouping/filtering must key off
    # 'dataset_size_str', not 'dataset', or every "size" bucket collapses
    # into the same filter and averages across all sizes.
    sizes = (
        df_family[['dataset_size', 'dataset_size_str']]
        .drop_duplicates()
        .sort_values('dataset_size')
    )

    datasets = sizes['dataset_size_str'].tolist()
    x_labels = sizes['dataset_size_str'].str.upper().tolist()

    fig, axes = plt.subplots(2, 2, figsize=(14, 10), constrained_layout=False)
    plot_name = "FinBench" if family.lower() == "finbench" else family.capitalize()
    fig.suptitle(
        f"Scalability {plot_name}",
        fontsize=SUPTITLE_SIZE
    )

    axes = axes.flatten()

    for i, rel in enumerate(relations):
        ax = axes[i]
        df_rel = df_family[df_family['relation'] == rel]

        if df_rel.empty:
            ax.text(
                0.5, 0.5, f"No data for {rel}",
                ha='center', va='center',
                transform=ax.transAxes,
                fontsize=EMPTY_TEXT_SIZE
            )
        else:
            x_base = np.arange(len(datasets))
            bar_width = 0.25

            for j, w in enumerate(windows):
                df_w = df_rel[df_rel['window'] == w]

                means = [
                    df_w[df_w['dataset_size_str'] == d]['query_time'].mean()
                    if not df_w[df_w['dataset_size_str'] == d].empty else 0.0
                    for d in datasets
                ]

                x_pos = x_base + (j - len(windows) // 2) * bar_width
                shade = ["light", "medium", "dark"][j % 3]

                ax.bar(
                    x_pos,
                    means,
                    width=bar_width,
                    color=colors[rel][shade],
                    label=f"w: {int(w)}",
                    alpha=0.85
                )

            ax.set_xticks(x_base)
            ax.set_xticklabels(x_labels)
            ax.set_yscale("log")

            # Axis tick formatting
            ax.tick_params(axis='x', labelsize=TICK_LABEL_SIZE)
            ax.tick_params(axis='y', which='both', labelsize=TICK_LABEL_SIZE)
            apply_consistent_sci_notation(ax)

            ax.legend(
                fontsize=LEGEND_SIZE,
                loc="upper left"
            )

        # Properly aligned relation title
        ax.set_title(f"Relation: {rel}", fontsize=RELATION_TITLE_SIZE, x=0.5, y=1.05)

    # Global axis labels (MATCHED to selectivity plots: x-label always shown,
    # y-label only shown once — on the finbench figure — since these plots
    # are meant to sit side-by-side sharing one y-axis label)
    fig.supxlabel('Dataset', fontsize=AXIS_LABEL_SIZE)
    if family.lower() == "finbench":
        fig.supylabel('Query Time (seconds)', fontsize=AXIS_LABEL_SIZE)

    # Align y labels across subplots
    fig.align_ylabels(axes.reshape(2, 2))

    plt.tight_layout()
    os.makedirs("scalability", exist_ok=True)
    plt.savefig(f"scalability/scalability_{family}.pdf", bbox_inches="tight")
    plt.show()

print("\nDone! Scalability plots generated with consistent exponential y-axis notation.")
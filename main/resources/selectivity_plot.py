from matplotlib.ticker import NullFormatter
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.ticker as mticker
import os

# Font sizes
SUPTITLE_SIZE = 40
RELATION_TITLE_SIZE = 35
AXIS_LABEL_SIZE = 35
TICK_LABEL_SIZE = 35
LEGEND_SIZE = 29
EMPTY_TEXT_SIZE = 20

# Load data
file = "main/resources/selectivity_time.csv"
df = pd.read_csv(file, sep=";")

# Normalize columns
for c in ['dataset', 'relation', 'ts']:
    if c in df.columns:
        df[c] = df[c].astype(str).str.strip()

for c in ['selectivity', 'window', 'query_time']:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors='coerce')

df['query_time'] = df['query_time'] / 1000.0

# Colors
colors = {
    "overlaps": {"light": "#FFD580", "medium": "#FFA500", "dark": "#CC5500"},
    "meets":    {"light": "#ADD8E6", "medium": "#0000FF", "dark": "#00008B"},
    "before":   {"light": "#90EE90", "medium": "#008000", "dark": "#006400"},
    "equal":    {"light": "#D8BFD8", "medium": "#800080", "dark": "#4B0082"}
}

time_series_mapping = {
    'synthea': 'bmi',
    'finbench': 'balance',
    'nyc': 'num_bikes_available',
    'la': 'hourly_speed',
    'bay': 'hourly_speed'
}

dataset_names = ['nyc', 'synthea', 'finbench', 'la', 'bay']

for dataset_name in dataset_names:

    ts_value = time_series_mapping.get(dataset_name)
    df_dataset = df[(df['dataset'] == dataset_name) & (df['ts'] == ts_value)]
    if df_dataset.empty:
        continue

    windows = sorted(df_dataset['window'].dropna().unique())
    fig, axes = plt.subplots(2, 2, figsize=(12, 9), constrained_layout=True)

    pretty_name = {
        "nyc": "NYC",
        "bay": "PEMS-BAY",
        "finbench": "FinBench",
        "synthea": "Synthea",
        "la": "METR-LA"
    }.get(dataset_name, dataset_name.capitalize())

    fig.suptitle(
        f"Selectivity {pretty_name}, ts:{ts_value}", fontsize=SUPTITLE_SIZE
    )
    fig.supxlabel('Query Selectivity', fontsize=AXIS_LABEL_SIZE)
    if dataset_name.lower() == "finbench":
        fig.supylabel('Query Time (seconds)', fontsize=AXIS_LABEL_SIZE)

    axes_flat = axes.flatten()
    relations = ['overlaps', 'before', 'meets', 'equal']

    for idx, rel in enumerate(relations):
        ax = axes_flat[idx]
        df_rel = df_dataset[df_dataset['relation'] == rel]

        if df_rel.empty:
            ax.text(
                0.5, 0.5, f'No data for {rel}',
                ha='center', va='center',
                transform=ax.transAxes,
                fontsize=EMPTY_TEXT_SIZE
            )
        else:
            all_selectivities = sorted(df_rel['selectivity'].dropna().unique())
            x_base = np.arange(len(all_selectivities))
            bar_width = 0.25

            for i, w in enumerate(windows):
                df_w = df_rel[df_rel['window'] == w]
                pivot = df_w.pivot_table(index='selectivity',
                                          values='query_time', aggfunc='mean')

                y_values = [
                    float(pivot.loc[sel].iloc[0]) if sel in pivot.index else 0.0
                    for sel in all_selectivities
                ]

                x_pos = x_base + (i - len(windows) // 2) * bar_width
                shade = ["light", "medium", "dark"][i % 3]

                ax.bar(
                    x_pos,
                    y_values,
                    width=bar_width,
                    color=colors[rel][shade],
                    label=f"w = {int(w)}",
                    alpha=0.8
                )

            ax.set_xticks(x_base)
            ax.set_xticklabels([f"Q{i+1}" for i in range(len(all_selectivities))])

            ax.tick_params(axis='x', labelsize=TICK_LABEL_SIZE)
            ax.tick_params(axis='y', labelsize=TICK_LABEL_SIZE)

            ax.yaxis.set_major_formatter(mticker.ScalarFormatter())
            ax.ticklabel_format(axis='y', style='sci', scilimits=(0,0))
            ax.yaxis.get_offset_text().set_fontsize(25)

            ax.legend(
                fontsize=LEGEND_SIZE,
                loc="upper left"
            )

        # 👇 Explicit fixed vertical title position (same for all)
        ax.set_title(f"Relation: {rel}",
                     fontsize=RELATION_TITLE_SIZE,
                     y=1.05)  # <-- fixed above the subplot frame

    # Align y labels
    fig.align_ylabels(axes[:, 0])
    fig.align_ylabels(axes[:, 1])

    # plt.show() 
    os.makedirs("selectivity", exist_ok=True)
    plt.savefig(f"selectivity/selectivity_{dataset_name}.pdf", bbox_inches="tight")

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

file = "../resources/selectivity_time.csv"
df = pd.read_csv(file, sep=";")

print("Loaded:", file)
print("Shape:", df.shape)
print("Columns:", df.columns.tolist())

# Normalize string columns
for c in ['dataset', 'relation', 'ts']:
    if c in df.columns:
        df[c] = df[c].astype(str).str.strip()

# Coerce numeric columns
for c in ['selectivity', 'window', 'query_time']:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors='coerce')

# Convert query_time from ms to seconds for plotting
if 'query_time' in df.columns:
    df['query_time'] = df['query_time'] / 1000.0

# Define colors for different relations with light, medium, and dark shades
colors = {
    "overlaps": {
        "light": "#FFD580",   # light orange
        "medium": "#FFA500",  # classic orange
        "dark": "#CC5500",    # dark burnt orange
    },
    "meets": {
        "light": "#ADD8E6",   # light blue
        "medium": "#0000FF",  # pure blue
        "dark": "#00008B",    # dark blue
    },
    "before": {
        "light": "#90EE90",   # light green
        "medium": "#008000",  # standard green
        "dark": "#006400",    # dark green
    },
    "equal": {
        "light": "#D8BFD8",   # thistle (light purple)
        "medium": "#800080",  # medium purple
        "dark": "#4B0082",    # indigo (dark purple)
    }
}

# Define the specific time series for each dataset
time_series_mapping = {
    'synthea': 'bmi',
    'finbench': 'balance',
    'nyc': 'num_bikes_available'
}

# Choose datasets to plot
dataset_names = ['nyc', 'synthea', 'finbench']

for dataset_name in dataset_names:
    print(f"\n{'='*50}")
    print(f"Processing dataset: {dataset_name}")
    print(f"{'='*50}")
    
    # Filter the dataset for the specific time series
    ts_value = time_series_mapping.get(dataset_name)
    df_dataset = df[(df['dataset'] == dataset_name) & (df['ts'] == ts_value)]
    print(f"Rows for dataset '{dataset_name}' with time series '{ts_value}':", df_dataset.shape[0])

    # Windows to plot
    windows = sorted(df_dataset['window'].dropna().unique())
    print("Windows found for dataset:", windows)

    # Skip if no data for this dataset
    if df_dataset.empty or not windows:
        print(f"No data found for dataset {dataset_name} with time series '{ts_value}', skipping...")
        continue

    # Create a single plot for the dataset
    fig, axes = plt.subplots(2, 2, figsize=(12, 9))
    fig.suptitle(f"Selectivity Analysis {'NYC' if dataset_name == 'nyc' else dataset_name.capitalize()} dataset, ts:{ts_value}", fontsize=18)
    
    # Flatten axes for easier iteration
    axes_flat = axes.flatten()
    all_relations = ['overlaps', 'before', 'meets', 'equal']  # Plot all 4 relations
    
    for idx, rel in enumerate(all_relations):
        ax = axes_flat[idx]
        df_rel = df_dataset[df_dataset['relation'] == rel]
        print(f"\nPlotting relation: {rel}, rows: {len(df_rel)}")
        
        if df_rel.empty:
            ax.text(0.5, 0.5, f'No data for {rel}', 
                    horizontalalignment='center', verticalalignment='center', 
                    transform=ax.transAxes, fontsize=14)
            ax.set_title(f"Relation: {rel}", fontsize=14)
            continue

        # Plot bars for each window
        all_selectivities = sorted(df_rel['selectivity'].dropna().unique())
        x_base = np.arange(len(all_selectivities))
        bar_width = 0.25  # Fixed bar width for consistency across all plots
        
        for i, w in enumerate(windows):
            df_w = df_rel[df_rel['window'] == w]
            if df_w.empty:
                continue

            pivot = df_w.pivot_table(index='selectivity', values='query_time', aggfunc='mean')
            y_values = [
                float(pivot.loc[sel].iloc[0]) if sel in pivot.index else 0.0
                for sel in all_selectivities
            ]
            x_pos = x_base + (i - len(windows)//2) * bar_width

            # Use the "light", "medium", or "dark" shade based on the window index
            shade = ["light", "medium", "dark"][i % 3]
            ax.bar(x_pos, y_values, width=bar_width, color=colors[rel][shade], 
                   label=f'w = {int(w)}', alpha=0.8)

        ax.set_xticks(x_base)
        labels = [f"Q{idx+1}" for idx, _ in enumerate(all_selectivities)]
        ax.set_xticklabels(labels)
        ax.set_xlabel('Query Selectivity', fontsize=12)
        ax.set_ylabel('Time (seconds)', fontsize=12)
        ax.set_yscale("log")
        ax.set_title(f"Relation: {rel}", fontsize=14)
        ax.legend(fontsize=10)

    plt.tight_layout()
    plt.savefig(f"{dataset_name}_{ts_value}.png", format='png', dpi=500)  # Save the plot
    plt.show()  # Display the plot

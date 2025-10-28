import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from itertools import cycle

# Update this path to your actual scalability CSV file
file = "../resources/scalability_time.csv"

try:
    df = pd.read_csv(file, sep=";")
    print("Loaded:", file)
    print("Shape:", df.shape)
    print("Columns:", df.columns.tolist())
except FileNotFoundError:
    print(f"File not found: {file}")
    print("Please update the file path in the script or copy the file to the workspace.")
    exit(1)


# Normalize string columns (adjust based on actual column names in your CSV)
string_columns = ['dataset', 'relation', 'ts'] if 'dataset' in df.columns else []
for c in string_columns:
    if c in df.columns:
        df[c] = df[c].astype(str).str.strip()

# Coerce numeric columns (adjust based on actual column names in your CSV)
numeric_columns = ['selectivity', 'window', 'query_time'] if 'query_time' in df.columns else []
# For scalability, common columns might be: 'data_size', 'execution_time', 'memory_usage', etc.
# Update this list based on your actual CSV structure
possible_numeric_cols = ['selectivity', 'window', 'query_time', 'data_size', 'execution_time', 'memory_usage', 'num_records', 'scale_factor']
for c in possible_numeric_cols:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors='coerce')

# Convert query_time from ms to seconds for plotting (if applicable)
if 'query_time' in df.columns:
    df['query_time'] = df['query_time'] / 1000.0
elif 'execution_time' in df.columns and df['execution_time'].max() > 1000:
    # Assume it's in milliseconds if values are large
    df['execution_time'] = df['execution_time'] / 1000.0

print("\nUnique values in key columns:")
for col in df.columns:
    if df[col].dtype == 'object' or col in ['dataset', 'relation', 'ts']:
        unique_vals = df[col].dropna().unique()
        print(f"{col}: {unique_vals[:10]}{'...' if len(unique_vals) > 10 else ''}")

# Define predefined colors for different relations and their shades
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

def generate_shades_from_predefined(relation, n_shades):
    """Generate shades using predefined color scheme"""
    if relation not in colors:
        # Fallback to blue shades if relation not found
        relation = "overlaps"
    
    relation_colors = colors[relation]
    shade_keys = list(relation_colors.keys())
    
    if n_shades <= len(shade_keys):
        # Use subset of available shades
        selected_keys = shade_keys[:n_shades]
        return [relation_colors[key] for key in selected_keys]
    else:
        # Repeat pattern if more shades needed than available
        shades = []
        for i in range(n_shades):
            key = shade_keys[i % len(shade_keys)]
            shades.append(relation_colors[key])
        return shades

# Process all datasets together for scalability analysis
# Since each dataset represents a different scale (small, normal, big, huge)
print(f"\n{'='*50}")
print(f"Creating scalability analysis across all dataset sizes")
print(f"{'='*50}")

print(f"Total rows:", df.shape[0])

# For this CSV structure, we want to plot query_time vs dataset size
# Since dataset represents different scales (small, normal, big, huge)
x_var = 'dataset'
y_var = 'query_time'
x_label = 'Dataset Size'
y_label = 'Query Time (seconds)'

print(f"Creating scalability plot: {y_var} vs {x_var}")

# Determine grouping variables
if 'relation' in df.columns:
    relations = sorted(df['relation'].dropna().unique())
    print("Relations found:", relations)
else:
    relations = ['all_data']

if 'window' in df.columns:
    windows = sorted(df['window'].dropna().unique())
    print("Windows found:", windows)
else:
    windows = [None]

if 'ts' in df.columns:
    ts_values = sorted(df['ts'].dropna().unique())
    print("Time series found:", ts_values)
else:
    ts_values = ['all_ts']

# Create a mapping for dataset order
dataset_order = ['finbench_5k', 'finbench_50k', 'finbench_160k', 'finbench_500k']
available_datasets = [d for d in dataset_order if d in df['dataset'].values]
print("Dataset order for plotting:", available_datasets)

# Create mapping from internal names to display names
dataset_display_names = {
    'finbench_5k': 'FinBench@5k',
    'finbench_50k': 'FinBench@50k',
    'finbench_160k': 'FinBench@160k',
    'finbench_500k': 'FinBench@500k'
}
display_labels = [dataset_display_names.get(d, d) for d in available_datasets]
print("Display labels:", display_labels)

# Create the plot - we'll show relations in subplots with histograms for each dataset
if len(relations) > 1:
    # Create 2x2 subplot for all relations (like the original plots.py)
    fig, axes = plt.subplots(2, 2, figsize=(12, 9))
    fig.suptitle(f"Scalability Analysis", fontsize=18)
    
    # Flatten axes for easier iteration
    axes_flat = axes.flatten()
    all_relations = ['overlaps', 'before', 'meets', 'equal']  # plot all 4 relations
    
    for idx, rel in enumerate(all_relations):
        ax = axes_flat[idx]
        df_rel = df[df['relation'] == rel]
        print(f"\\nPlotting relation: {rel}, rows: {len(df_rel)}")
        
        if df_rel.empty:
            ax.text(0.5, 0.5, f'No data for {rel}', 
                   horizontalalignment='center', verticalalignment='center', 
                   transform=ax.transAxes, fontsize=14)
            ax.set_title(f"Relation: {rel}", fontsize=14)
            continue

        # Get colors for this relation using the same scheme as plots.py
        window_shades = generate_shades_from_predefined(rel, len(windows))
        window_colors_rel = {w: window_shades[i] for i, w in enumerate(windows)}
        
        # Get all unique datasets to create grouped histograms
        x_base = np.arange(len(available_datasets))
        bar_width = 0.25  # Fixed bar width for consistency across all plots (same as plots.py)
        
        # For each window, create bars for all datasets
        for i, w in enumerate(windows):
            df_w = df_rel[df_rel['window'] == w]
            if df_w.empty:
                continue
                
            # Calculate mean query time for each dataset
            dataset_means = []
            for dataset in available_datasets:
                df_dataset = df_w[df_w['dataset'] == dataset]
                if not df_dataset.empty:
                    mean_time = df_dataset['query_time'].mean()
                    dataset_means.append(float(mean_time))  # Convert to float like plots.py
                else:
                    dataset_means.append(0.0)  # Use float zero like plots.py
            
            # Position bars side by side (same centering logic as plots.py)
            x_pos = x_base + (i - len(windows)//2) * bar_width
            ax.bar(x_pos, dataset_means, width=bar_width, 
                  color=window_colors_rel.get(w, 'gray'), 
                  label=f'w = {int(w)}', alpha=0.8)

        ax.set_xticks(x_base)
        ax.set_xticklabels(display_labels)
        ax.set_xlabel('Dataset', fontsize=12)
        ax.set_ylabel('Time (seconds)', fontsize=12)  # Match plots.py label
        ax.set_yscale('log')  # Add log scale for better visualization
        ax.set_title(f"Relation: {rel}", fontsize=14)
        ax.legend(fontsize=10)

    plt.tight_layout()
    plt.savefig(f"scalability_analysis.png", format='png', dpi=500)
    print(f"Saved plot: scalability_analysis.png")
    plt.show()

else:
    # Single plot for all data (fallback case)
    fig, ax = plt.subplots(1, 1, figsize=(10, 6))
    fig.suptitle(f"Scalability Analysis", fontsize=18)

    # Get colors for datasets using the same scheme as plots.py
    window_shades = generate_shades_from_predefined("overlaps", len(windows))
    window_colors_rel = {w: window_shades[i] for i, w in enumerate(windows)}

    # Get all unique datasets to create grouped histograms
    x_base = np.arange(len(available_datasets))
    bar_width = 0.25  # Fixed bar width for consistency (same as plots.py)

    # For each window, create bars for all datasets
    for i, w in enumerate(windows):
        df_w = df[df['window'] == w]
        if df_w.empty:
            continue

        # Calculate mean query time for each dataset
        dataset_means = []
        for dataset in available_datasets:
            df_dataset = df_w[df_w['dataset'] == dataset]
            if not df_dataset.empty:
                mean_time = df_dataset['query_time'].mean()
                dataset_means.append(float(mean_time))  # Convert to float like plots.py
            else:
                dataset_means.append(0.0)  # Use float zero like plots.py

        # Position bars side by side (same centering logic as plots.py)
        x_pos = x_base + (i - len(windows)//2) * bar_width
        ax.bar(x_pos, dataset_means, width=bar_width,
               color=window_colors_rel.get(w, 'gray'),
               label=f'w = {int(w)}', alpha=0.8)

    ax.set_xticks(x_base)
    ax.set_xticklabels(display_labels)
    ax.set_xlabel('Dataset', fontsize=12)
    ax.set_ylabel('Time (seconds)', fontsize=12)  # Match plots.py label
    ax.set_yscale('log')  # Add log scale for better visualization
    ax.legend(fontsize=10)

    plt.tight_layout()

    # Save the plot
    output_filename = f"../../../DottoratoCode/scalability_analysis.png"
    plt.savefig(output_filename, format='png', dpi=500)
    print(f"Saved plot: {output_filename}")
    plt.show()

print("\nScalability plotting completed!")


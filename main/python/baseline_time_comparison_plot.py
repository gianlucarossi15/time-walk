import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

df = pd.read_csv("../resources/baseline_times.csv",header=0,sep=';')

# normalize numeric columns (handles stray spaces like " 9.251")
print(df.columns)

tw_col = 'timewalk_time'
print(df[tw_col])

# --- CHANGED: coerce time columns to numeric and aggregate by dataset ---
# convert possible string/whitespace values to numeric
df[tw_col] = pd.to_numeric(df[tw_col].astype(str).str.strip(), errors='coerce')
df['ts_join_time'] = pd.to_numeric(df['ts_join_time'].astype(str).str.strip(), errors='coerce')
df['neo4j_time'] = pd.to_numeric(df['neo4j_time'].astype(str).str.strip(), errors='coerce')

# --- CHANGED: convert from milliseconds to seconds by dividing by 1000 ---
df[tw_col] = df[tw_col] / 1000.0
df['ts_join_time'] = df['ts_join_time'] / 1000.0
df['neo4j_time'] = df['neo4j_time'] / 1000.0

# compute combined join+neo4j time from numeric values (now in seconds)
df['join_plus_neo4j'] = df['ts_join_time'] + df['neo4j_time']

# group by dataset (preserve appearance order) and take mean for each dataset
grouped = df.groupby('dataset', sort=False, as_index=False)[[tw_col, 'join_plus_neo4j']].mean()


sizes = grouped['dataset'].astype(str).str.lower()
m = sizes.str.extract(r'(\d+(?:\.\d*)?)\s*([k]?)', expand=True)
num = pd.to_numeric(m[0], errors='coerce')
mult = np.where(m[1].fillna('') == 'k', 1000, 1)
grouped['size'] = (num * mult).astype('Int64')

allowed = [5000, 10000, 30000, 100000]
filtered = grouped[grouped['size'].isin(allowed)].copy()

# preserve requested order
order_map = {v: i for i, v in enumerate(allowed)}
filtered['order'] = filtered['size'].map(order_map)
filtered.sort_values('order', inplace=True)
plot_df = filtered.reset_index(drop=True)

# drop rows where both times are NaN (no data for that dataset)
plot_df = plot_df.dropna(how='all', subset=[tw_col, 'join_plus_neo4j'])

if plot_df.empty:
    raise RuntimeError("No numeric rows available to plot after cleaning.")

labels = plot_df['dataset'].astype(str).tolist()
new_labels = ["synthea@5k", "synthea@10k", "synthea@30k", "synthea@100k"]

# Ensure plot_df matches the datasets in new_labels or FinBench labels
all_labels = new_labels + ["finbench@5k", "finbench@50k", "finbench@160k", "finbench@500k"]
plot_df = plot_df[plot_df['dataset'].isin(all_labels)].copy()

# Ensure x matches the number of labels
x = list(range(len(all_labels)))  # Adjust x to match the length of all_labels

y1 = plot_df[tw_col].values
y2 = plot_df['join_plus_neo4j'].values

# Calculate speedup as the log difference between join_plus_neo4j and time-walk
speedup = np.log10(plot_df['join_plus_neo4j']) - np.log10(plot_df[tw_col])
max_speedup = speedup.max()

print(f"Maximum speedup (log scale): {max_speedup}")

# Calculate the maximum division (ratio) between join_plus_neo4j and time-walk
ratios = plot_df['join_plus_neo4j'] / plot_df[tw_col]
max_ratio = ratios.max()

print(f"Maximum division (ratio): {max_ratio}")

# --- CHANGED: use default color palette ---
timewalk_color = 'blue'  # Default color for Time-Walk
baseline_color = 'orange'  # Default color for Baseline

# --- CHANGED: Create a single plot with two histograms per dataset ---
plt.figure(figsize=(12, 6))

bar_width = 0.35  # Width of each bar
x = np.arange(len(new_labels))  # X positions for Synthea datasets

# Plot histograms for Time-Walk and Baseline
timewalk_means = plot_df[plot_df['dataset'].isin(new_labels)][tw_col].values
baseline_means = plot_df[plot_df['dataset'].isin(new_labels)]['join_plus_neo4j'].values

# Create bars for Time-Walk
plt.bar(x - bar_width / 2, timewalk_means, width=bar_width, color=timewalk_color, label='Time-Walk')

# Create bars for Baseline
plt.bar(x + bar_width / 2, baseline_means, width=bar_width, color=baseline_color, label='Baseline')

# Set log scale for y-axis
plt.yscale('log')

# Add labels, title, and legend
plt.xlabel('Dataset', fontdict={'size': 12})
plt.ylabel('Time (seconds)', fontdict={'size': 12})
plt.title('Synthea Mirrored query time vs Baseline', fontsize=14)
plt.xticks(x, new_labels, fontsize=10)
plt.legend(fontsize=10)
plt.tight_layout()
plt.show()

# --- CHANGED: Plot FinBench data in a separate figure ---
finbench_labels = ["finbench@5k", "finbench@50k", "finbench@160k", "finbench@500k"]
finbench_df = plot_df[plot_df['dataset'].isin(finbench_labels)].copy()

if not finbench_df.empty:
    x_finbench = np.arange(len(finbench_labels))  # X positions for FinBench datasets
    timewalk_means_finbench = finbench_df[tw_col].values
    baseline_means_finbench = finbench_df['join_plus_neo4j'].values

    plt.figure(figsize=(12, 6))

    # Create bars for Time-Walk
    plt.bar(x_finbench - bar_width / 2, timewalk_means_finbench, width=bar_width, color=timewalk_color, label='Time-Walk')

    # Create bars for Baseline
    plt.bar(x_finbench + bar_width / 2, baseline_means_finbench, width=bar_width, color=baseline_color, label='Baseline')

    # Set log scale for y-axis
    plt.yscale('log')

    # Add labels, title, and legend
    plt.xlabel('Dataset', fontdict={'size': 12})
    plt.ylabel('Time (seconds)', fontdict={'size': 12})
    plt.title('FinBench Mirrored query time vs Baseline', fontsize=14)
    plt.xticks(x_finbench, finbench_labels, fontsize=10)
    plt.legend(fontsize=10)
    plt.tight_layout()
    plt.show()
else:
    print("No FinBench data available to plot.")
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

file = "../resources/selectivity_time.csv"
df = pd.read_csv(file, sep=";")

x = np.arange(len(df))  # x locations for each row
width = 0.4             # width of each bar

fig, ax = plt.subplots(figsize=(10,6))

# Bars for tgt_time (shifted left)
bars1 = ax.bar(x - width/2, df["tgt_time"], width, label="tgt_time")

# Bars for qof_time (shifted right)
bars2 = ax.bar(x + width/2, df["qof_time"], width, label="qof_time")

# Add labels on top of bars
for bar in bars1:
    height = bar.get_height()
    ax.annotate(f'{height:.1f}',
                xy=(bar.get_x() + bar.get_width() / 2, height),
                xytext=(0, 3),  # 3 points vertical offset
                textcoords="offset points",
                ha='center', va='bottom')

for bar in bars2:
    height = bar.get_height()
    ax.annotate(f'{height:.1f}',
                xy=(bar.get_x() + bar.get_width() / 2, height),
                xytext=(0, 3),
                textcoords="offset points",
                ha='center', va='bottom')

# Labels, legend, formatting
ax.set_xticks(x)
ax.set_xticklabels([f"Workload {i+1}" for i in df.index])
ax.set_xticklabels(["Workload 1 (relation changes)", "Workload 2 (time series type changes)"])
ax.set_ylabel("Time (ms)")
ax.set_xlabel("Workloads")
ax.set_title("Finbench Execution Times, Mirrored queries")
ax.legend()

plt.tight_layout()
plt.savefig("finbench/Finbench_execution_times_mirrored_queries.png")
plt.show()
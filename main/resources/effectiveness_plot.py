import os
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import ScalarFormatter

# ---------------------------------------------------------------------------
# 0. Font sizes
# ---------------------------------------------------------------------------
AXIS_LABEL_SIZE = 22
TITLE_SIZE = 28
TICK_LABEL_SIZE = 22
LEGEND_SIZE = 20

# ---------------------------------------------------------------------------
# 1. Load data
# ---------------------------------------------------------------------------
df = pd.read_csv("main/resources/baseline_time.csv", sep=";")
df["query_time_s"] = df["query_time"] / 1000.0  # ms -> seconds

OUTPUT_DIR = "effectiveness"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# 2. Helpers: consistent ordering of dataset sizes + method style
# ---------------------------------------------------------------------------
def size_key(s: str) -> float:
    s = s.lower()
    if s.endswith("k"):
        return float(s[:-1]) * 1_000
    if s.endswith("m"):
        return float(s[:-1]) * 1_000_000
    return float(s)

METHOD_STYLE = {
    "STAMP":   dict(label="stamp",   color="tab:blue",   marker="o"),
    "STOMP":   dict(label="stomp",   color="tab:orange", marker="s"),
    "TD-Join": dict(label="td-join", color="tab:green",  marker="^"),
}
METHOD_ORDER = ["STAMP", "STOMP", "TD-Join"]  # legend order
RELATION_ORDER = ["overlaps", "before", "meets", "equal"]

# ---------------------------------------------------------------------------
# 3. Plotting function for one dataset/relation panel
# ---------------------------------------------------------------------------
def plot_relation(ax, data, dataset_name, relation):
    sub = data[(data["dataset"] == dataset_name) & (data["relation"] == relation)].copy()
    sizes = sorted(sub["datasetSize"].unique(), key=size_key)

    for method in METHOD_ORDER:
        m_sub = sub[sub["method"] == method].set_index("datasetSize")
        y = [m_sub.loc[s, "query_time_s"] for s in sizes]
        style = METHOD_STYLE[method]
        ax.plot(sizes, y, marker=style["marker"], color=style["color"],
                label=style["label"], linewidth=1.5, markersize=6)

    ax.set_yscale("log")
    ax.yaxis.set_major_formatter(ScalarFormatter())
    ax.yaxis.get_major_formatter().set_scientific(True)
    ax.yaxis.get_major_formatter().set_powerlimits((0, 0))

    ax.set_title(f"Relation: {relation}", fontsize=TITLE_SIZE)
    ax.set_xlabel("Dataset Size", fontsize=AXIS_LABEL_SIZE)
    ax.set_ylabel("Query Time (seconds)", fontsize=AXIS_LABEL_SIZE)
    ax.tick_params(axis="both", which="major", labelsize=TICK_LABEL_SIZE)
    ax.yaxis.get_offset_text().set_fontsize(TICK_LABEL_SIZE)
    ax.legend(loc="upper left", fontsize=LEGEND_SIZE)
    ax.grid(False)

# ---------------------------------------------------------------------------
# 4. One figure per dataset, 2x2 grid of relations
# ---------------------------------------------------------------------------
DATASETS = ["synthea", "finbench"]

for dataset_name in DATASETS:
    fig, axes = plt.subplots(2, 2, figsize=(12, 10))

    for ax, relation in zip(axes.flat, RELATION_ORDER):
        plot_relation(ax, df, dataset_name, relation)

    # fig.suptitle(f"Effectiveness with {dataset_name.capitalize()}", fontsize=TITLE_SIZE)
    # fig.tight_layout()
    # fig.savefig(os.path.join(OUTPUT_DIR, f"effectiveness_{dataset_name}.pdf"))

    plt.show()
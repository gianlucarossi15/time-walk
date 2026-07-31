"""
plot_per_path_cost.py

Plots results from tkde_per_path_cost.csv (PerPathCost.java experiment, E1).

Validates Table III: per-hop query time under PAIRWISE-CONTINUOUS ("2cont")
should grow roughly linearly in path length n, while CONTINUOUS ("cont")
should stay roughly flat after the first hop (source pinning collapses the
per-hop scan cost).

Axis conventions:
  - y-axis: query_time is stored in milliseconds in the CSV and converted
    to seconds for plotting.
  - x-axis: path length n (hops) only ever takes integer values, so ticks
    are forced to integers.

By default the script now plots all four Allen's relations retained in the
paper (before, overlaps, meets, equal), producing one figure (and one
speedup figure) per (dataset, relation) pair. Use --relation to restrict to
a subset.

Usage:
    python plot_per_path_cost.py --dataset bay --dataset la
    python plot_per_path_cost.py --dataset bay --relation overlaps --relation before

Defaults to reading from
/Users/gianluca/github/Edge2Time/src/main/resources/tkde_per_path_cost.csv
and writing figures to ./per_path_cost/ ; override with --csv-path / --out-dir.

Produces one figure per (dataset, relation): query_time (seconds) vs n, one
line per (semantics, w) combination, faceted by time series if a dataset has
more than one. Filenames follow <fig-label>.pdf so they can be included via
\\includegraphics and referenced with \\label{fig:<name>} / \\ref{fig:<name>}
in LaTeX (label convention: sec:name for section/subsection/subsubsection,
fig:name for figures, alg:name for algorithms).
"""

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

AXIS_LABEL_SIZE = 20
TITLE_SIZE = 22
TICK_LABEL_SIZE = 18
LEGEND_SIZE = 12

SEMANTICS_LABELS = {"2cont": "PW-CONT", "cont": "CONT"}
SEMANTICS_STYLE = {"2cont": {"linestyle": "--", "marker": "o"},
                    "cont": {"linestyle": "-", "marker": "s"}}
WINDOW_COLORS = {30: "tab:blue", 50: "tab:orange", 70: "tab:green",
                  4: "tab:blue", 5: "tab:orange", 7: "tab:green"}

PRETTY_NAME = {
    "nyc": "NYC",
    "bay": "PEMS-BAY",
    "finbench": "FinBench",
    "synthea": "Synthea",
    "la": "METR-LA",
}

# The four Allen's relations retained in the paper (equal, before, overlaps,
# meets; see Table II). This is the default set plotted for every dataset.
ALLEN_RELATIONS = ["overlaps", "equal","before"]


def load_data(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path, sep=";")
    df.columns = [c.strip() for c in df.columns]

    for c in ["dataset", "semantics", "relation", "ts"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    for c in ["n", "window", "td_join_time_ms"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # CSV stores query_time in milliseconds; convert to seconds for plotting.
    df["td_join_time_ms"] = df["td_join_time_ms"] / 1000.0
    return df


def _style_axes(ax) -> None:
    ax.tick_params(axis="x", labelsize=TICK_LABEL_SIZE)
    ax.tick_params(axis="y", labelsize=TICK_LABEL_SIZE)
    # n is a hop count -> integers only on the x-axis
    ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax.grid(True, alpha=0.3)


def plot_dataset(df: pd.DataFrame, dataset: str, relation: str,
                  out_dir: Path, log_scale: bool = False) -> None:
    sub = df[(df["dataset"] == dataset) & (df["relation"] == relation)]
    if sub.empty:
        print(f"[skip] no rows for dataset={dataset} relation={relation}")
        return

    ts_values = sorted(sub["ts"].unique())
    n_ts = len(ts_values)
    pretty_name = PRETTY_NAME.get(dataset, dataset.capitalize())

    fig, axes = plt.subplots(1, n_ts, figsize=(6 * n_ts, 4.5), squeeze=False,
                              constrained_layout=True)
    axes = axes[0]

    fig.suptitle(f"Semantics Comparison {pretty_name}", fontsize=TITLE_SIZE)

    for ax, ts in zip(axes, ts_values):
        ts_sub = sub[sub["ts"] == ts]

        for sem in ["2cont", "cont"]:
            sem_sub = ts_sub[ts_sub["semantics"] == sem]
            for w in sorted(sem_sub["window"].unique()):
                w_sub = sem_sub[sem_sub["window"] == w].sort_values("n")
                if w_sub.empty:
                    continue
                style = SEMANTICS_STYLE[sem]
                color = WINDOW_COLORS.get(w, None)
                ax.plot(
                    w_sub["n"], w_sub["td_join_time_ms"],
                    label=f"{SEMANTICS_LABELS[sem]}, w={int(w)}",
                    color=color, linewidth=2, markersize=6, **style,
                )

        ax.set_xlabel("Path Length n (hops)", fontsize=AXIS_LABEL_SIZE)
        ax.set_ylabel("Query Time (seconds)", fontsize=AXIS_LABEL_SIZE)
        ax.set_title(f"ts: {ts}", fontsize=TITLE_SIZE)
        # if log_scale:
        #     ax.set_yscale("log")
        _style_axes(ax)
        ax.legend(fontsize=LEGEND_SIZE, loc="upper left")

    out_path = out_dir / f"per_path_cost_{dataset}_{relation}.pdf"
    fig.savefig(out_path)
    print(f"[saved] {out_path}  (\\label{{fig:per-path-cost-{dataset}-{relation}}})")
    # plt.close(fig)
    # plt.show()


def plot_speedup(df: pd.DataFrame, dataset: str, relation: str, out_dir: Path) -> None:
    """
    Plots the PW-CONT / CONT speedup ratio per hop n, one line per window w.
    Complements Table III: expected ratio grows roughly with n for PW-CONT
    (linear cost) vs flat CONT cost, so the ratio itself should grow with n.
    """
    sub = df[(df["dataset"] == dataset) & (df["relation"] == relation)]
    if sub.empty:
        return

    pivot = sub.pivot_table(
        index=["ts", "window", "n"], columns="semantics", values="td_join_time_ms"
    ).reset_index()
    if "2cont" not in pivot.columns or "cont" not in pivot.columns:
        print(f"[skip speedup] missing semantics columns for {dataset}/{relation}")
        return

    pivot["speedup"] = pivot["2cont"] / pivot["cont"].replace(0, pd.NA)
    pretty_name = PRETTY_NAME.get(dataset, dataset.capitalize())

    fig, ax = plt.subplots(figsize=(6.5, 4.5), constrained_layout=True)
    for (ts, w), grp in pivot.groupby(["ts", "window"]):
        grp = grp.sort_values("n")
        ax.plot(grp["n"], grp["speedup"], marker="o", linewidth=2, markersize=6,
                label=f"ts={ts}, w={int(w)}")

    ax.set_xlabel("Path Length n (hops)", fontsize=AXIS_LABEL_SIZE)
    ax.set_ylabel("Speedup", fontsize=AXIS_LABEL_SIZE)
    ax.set_title(f"Speedup (PW-CONT / CONT) {pretty_name} ({relation})", fontsize=TITLE_SIZE)
    ax.axhline(1.0, color="gray", linestyle=":", linewidth=1)
    _style_axes(ax)
    ax.legend(fontsize=LEGEND_SIZE)

    out_path = out_dir / f"speedup_{dataset}_{relation}.pdf"
    # fig.savefig(out_path, bbox_inches="tight")
    # print(f"[saved] {out_path}  (\\label{{fig:speedup-{dataset}-{relation}}})")
    # plt.close(fig)
    # plt.show()


def main():
    default_csv = "/Users/gianluca/github/Edge2Time/src/main/resources/tkde_per_path_cost.csv"

    parser = argparse.ArgumentParser(description="Plot per-path cost experiment results")
    parser.add_argument("--csv-path", default=default_csv,
                         help=f"Path to tkde_per_path_cost.csv (default: {default_csv})")
    parser.add_argument("--dataset", action="append", default=None,
                         help="Dataset(s) to plot (repeatable). Default: all datasets in file.")
    parser.add_argument("--relation", action="append", default=None,
                         help="Allen relation(s) to plot (repeatable). "
                              f"Default: all four relations {ALLEN_RELATIONS}.")
    parser.add_argument("--out-dir", default="per_path_cost", help="Output directory for figures")
    # parser.add_argument("--log", action="store_true", help="Use log scale for query_time axis")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = load_data(args.csv_path)

    datasets = args.dataset if args.dataset else sorted(df["dataset"].unique())
    relations = args.relation if args.relation else ALLEN_RELATIONS
    for dataset in datasets:
        for relation in relations:
            plot_dataset(df, dataset, relation, out_dir)
            plot_speedup(df, dataset, relation, out_dir)


if __name__ == "__main__":
    main()
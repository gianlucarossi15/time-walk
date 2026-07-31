import os
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from shutil import copy

from balance_generation import simulate_balance_by_account_level
from interest_rate_generation import simulate_interest_rate  # kept for parity

neo4j_path = '/Users/gianluca/neo4j-enterprise-5.26.12/'
datasets = ["sf0.1", "sf0.3", "sf1", "sf3"]

SAMPLE_FRAC = 0.5
RANDOM_STATE = 42
N_WORKERS = max(1, (os.cpu_count() or 2) - 1)
CHUNKSIZE = 200  # tune: bigger = less IPC overhead, smaller = better load balance


def _simulate_one(args):
    account_id, level, start_date = args
    ts = simulate_balance_by_account_level(level=level, start_date=start_date)

    timestamps = [
        t[0].strftime("%Y-%m-%dT%H:%M:%S") + ("+0000" if not t[0].tzinfo else "")
        for t in ts
    ]
    values = [
        round(float(v), 3) if isinstance(v, (int, float)) else v
        for _, v in ts
    ]
    return {
        "id": account_id,
        "balance_timestamps": timestamps,
        "balance_values": values,
    }


def process_dataset(ds, executor):
    file_path = f"{neo4j_path}import/{ds}/snapshot/Account.csv"
    full_df = pd.read_csv(file_path, sep='|')

    records_df = full_df.sample(frac=SAMPLE_FRAC, random_state=RANDOM_STATE).copy()

    records_df['createTime'] = pd.to_datetime(
        records_df['createTime'].astype(str),
        format='mixed',
        errors='coerce'
    ).dt.date

    n_bad_dates = records_df['createTime'].isna().sum()
    if n_bad_dates:
        print(f"  [warn] {ds}: {n_bad_dates} rows had unparseable createTime (set to NaT)")

    # Build picklable argument tuples (avoid sending whole rows over IPC)
    work = list(zip(
        records_df['accountId'].tolist(),
        records_df['accountLevel'].tolist(),
        records_df['createTime'].tolist(),
    ))

    results = list(executor.map(_simulate_one, work, chunksize=CHUNKSIZE))

    results_df = pd.DataFrame(results)
    output_csv = f"account_balances_{ds}.csv"
    results_df.to_csv(output_csv, index=False)

    print(f"{ds}: full={len(full_df)}, sampled={len(records_df)} "
          f"({len(records_df) / len(full_df):.1%} of total)")

    dest_path = f"{neo4j_path}import/{ds}/snapshot/account_balances.csv"
    copy(output_csv, dest_path)


if __name__ == "__main__":
    with ProcessPoolExecutor(max_workers=N_WORKERS) as executor:
        for ds in datasets:
            process_dataset(ds, executor)
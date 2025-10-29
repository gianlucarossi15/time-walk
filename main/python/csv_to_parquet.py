import pandas as pd

def csv_to_parquet(csv_path: str, parquet_path: str,
                   engine: str = "pyarrow",
                   compression: str = "snappy"):
    # Read CSV
    df = pd.read_csv(csv_path)
    # Write Parquet
    df.to_parquet(parquet_path, engine=engine, compression=compression, index=False)
    print(f"✅ Wrote Parquet to {parquet_path}")

if __name__ == "__main__":
    dataset = "dataset/name"
    neo4j_path ="paths/to/neo4j"
    csv_path = f"{neo4j_path}/import/synthea/timeseries_{dataset}.csv"
    parquet_path = f"timeseries_{dataset}.parquet"
    csv_to_parquet(csv_path, parquet_path)
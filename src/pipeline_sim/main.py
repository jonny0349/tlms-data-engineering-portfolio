import os, pandas as pd, pyarrow as pa, pyarrow.parquet as pq
from .transforms import bronze_chart_to_silver, bronze_rwis_to_silver, bronze_wzdx_to_silver, build_gold_fact

BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA = os.path.join(BASE, "data")
BRONZE = os.path.join(DATA, "bronze")
SILVER = os.path.join(DATA, "silver")
GOLD = os.path.join(DATA, "gold")

def _to_parquet(df: pd.DataFrame, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if df.empty:
        table = pa.Table.from_pandas(df, preserve_index=False)
    else:
        table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, path)

def run():
    # Bronze: minimal copy (here we just reference sample files)
    samples = {
        "chart": os.path.join(DATA, "samples", "chart_incidents.jsonl"),
        "rwis": os.path.join(DATA, "samples", "rwis_observations.jsonl"),
        "wzdx": os.path.join(DATA, "samples", "wzdx_sample.geojson"),
    }
    # Silver
    chart_s = bronze_chart_to_silver(samples["chart"])
    rwis_s = bronze_rwis_to_silver(samples["rwis"])
    wzdx_s = bronze_wzdx_to_silver(samples["wzdx"])

    _to_parquet(chart_s, os.path.join(SILVER, "chart_incidents.parquet"))
    _to_parquet(rwis_s, os.path.join(SILVER, "rwis_observations.parquet"))
    _to_parquet(wzdx_s, os.path.join(SILVER, "wzdx_features.parquet"))

    # Gold
    fact = build_gold_fact(chart_s, rwis_s, wzdx_s)
    _to_parquet(fact, os.path.join(GOLD, "fact_traffic_events.parquet"))
    print("Wrote silver & gold parquet datasets.")

if __name__ == "__main__":
    run()

import sys, duckdb

def main():
    if len(sys.argv) < 2:
        print("Usage: python -m src.pipeline_sim.ducksql "SELECT 1"")
        sys.exit(1)
    q = sys.argv[1]
    con = duckdb.connect()
    res = con.execute(q).fetchdf()
    try:
        from caas_jupyter_tools import display_dataframe_to_user as disp
        disp("DuckDB Query Result", res)
    except Exception:
        print(res.head())

if __name__ == "__main__":
    main()

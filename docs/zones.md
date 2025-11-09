# Zone definitions

- **Bronze (raw):** Immutable ingestion in native structure. Store as JSON/GeoJSON/CSV; convert to Parquet with minimal typing.
- **Silver (validated/curated):** Enforced contracts, normalized column names, typed fields, and deduplication.
- **Gold (analytics-ready):** Star schema: `fact_traffic_events` joined with `dim_road_segment`, `dim_weather`, `dim_workzone` by keys and time.

Validation highlights:
- Timestamps → ISO8601 UTC
- Geo → WGS84 (EPSG:4326)
- IDs → surrogate keys via hashing
- Strings trimmed; enums uppercased
- Drop/flag rows failing contracts

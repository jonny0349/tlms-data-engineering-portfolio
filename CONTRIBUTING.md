# Contributing

- Use feature branches and conventional commits.
- Keep functions pure where possible; add unit tests for transforms.
- Update `docs/source_mappings.md` when schemas change.
- Prefer Parquet + columnar ops; avoid row-by-row loops in transforms.

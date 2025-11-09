# Data Governance

- **Lineage:** Document source→bronze→silver→gold in `docs/source_mappings.md`.
- **Quality gates:** Pydantic contracts + unit tests in `tests/`.
- **Security (AWS target):** Lake Formation policies by role; column-level permissions for PII.
- **Observability:** Metrics (row counts, error rates), data freshness, and schema drift alerts (future TODO).

# Storage

Domain scope: persistence utilities (Parquet/Hive layout, retention helpers).

Current contents:
- Parquet helpers and retention utilities.

Migration notes:
- Ensure writers drop partition columns before persistence; readers use `hive_partitioning=False` where needed.
- Centralize lake path conventions and compression defaults.

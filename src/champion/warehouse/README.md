# Warehouse

Domain scope: ClickHouse integration (clients, loaders, queries, models).

Current contents:
- ClickHouse subpackage scaffold; loader code pending move from `warehouse/loader`.

Migration notes:
- Move batch loader and related tests here.
- Add query helpers and typed models under `models/`.
- Keep DSN/config handling centralized.

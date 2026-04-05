# Data Quality SQL

Папка с SQL-проверками качества данных для DWH-слоев `ods` и `dm`.

- `01_layer_reconciliation.sql` - сверка ODS и FACT по объему и по дням.
- `02_fact_fk_integrity.sql` - проверка NULL/FK и ссылочной целостности факта.
- `03_dimension_uniqueness.sql` - проверка уникальности ключевых измерений.

Ожидаемые результаты для корректной загрузки:

- расхождение ODS vs FACT = `0`
- `fact_fk_nulls = 0`
- `broken_dim_links = 0`
- дубликаты в уникальных комбинациях dimension = `0` строк

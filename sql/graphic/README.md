# SQL for Metabase Visualizations

Набор SQL-запросов для карточек в Metabase.

Файлы в этой папке:

- `01_daily_events.sql` - количество событий по дням.
- `02_daily_events_with_ma7.sql` - динамика по дням + скользящее среднее 7 дней.
- `03_top_regions.sql` - топ регионов по количеству событий и средней магнитуде.
- `04_magnitude_distribution.sql` - распределение по диапазонам магнитуды.
- `05_depth_distribution.sql` - распределение по диапазонам глубины.
- `06_status_distribution.sql` - распределение по статусам событий.
- `07_recent_strong_events.sql` - последние сильные события (`mag >= 5`).
- `08_map_points.sql` - точки для карты (lat/lon, count, avg mag).
- `09_heatmap_day_hour.sql` - матрица день недели x час (для pivot/heatmap).

Во всех запросах, где это уместно, используются переменные Metabase:

- `{{date_from}}`
- `{{date_to}}`

Рекомендуемый тип переменных: `Date`.

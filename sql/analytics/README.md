# Analytics SQL Examples

Примеры аналитических SQL-запросов для исследования землетрясений.

- `02_top_regions_and_intensity.sql` - топ регионов, средняя/максимальная магнитуда, события `mag >= 5`.
- `03_daily_weekly_trends.sql` - дневной и недельный тренды по событиям и средней магнитуде.
- `04_event_type_and_status_analysis.sql` - срез по типам событий и статусам.
- `05_depth_vs_magnitude.sql` - кросс-анализ глубины и магнитуды по бакетам.
- `06_recent_significant_events.sql` - последние значимые события (`mag >= 5`).

Проверки качества данных вынесены отдельно: `../data_quality/`.

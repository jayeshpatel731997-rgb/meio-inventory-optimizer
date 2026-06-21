WITH expected(table_name) AS (
    VALUES
        ('mart_demand_stats'),
        ('mart_inventory_position'),
        ('mart_cost_to_serve'),
        ('mart_network_flow'),
        ('mart_data_quality_report')
),
existing AS (
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_name IN (
          'mart_demand_stats',
          'mart_inventory_position',
          'mart_cost_to_serve',
          'mart_network_flow',
          'mart_data_quality_report'
      )
),
row_counts AS (
    SELECT 'mart_demand_stats' AS table_name, COUNT(*)::bigint AS row_count FROM public.mart_demand_stats
    UNION ALL
    SELECT 'mart_inventory_position', COUNT(*)::bigint FROM public.mart_inventory_position
    UNION ALL
    SELECT 'mart_cost_to_serve', COUNT(*)::bigint FROM public.mart_cost_to_serve
    UNION ALL
    SELECT 'mart_network_flow', COUNT(*)::bigint FROM public.mart_network_flow
    UNION ALL
    SELECT 'mart_data_quality_report', COUNT(*)::bigint FROM public.mart_data_quality_report
)
SELECT
    e.table_name,
    existing.table_name IS NOT NULL AS exists,
    row_counts.row_count
FROM expected e
LEFT JOIN existing
    ON e.table_name = existing.table_name
LEFT JOIN row_counts
    ON e.table_name = row_counts.table_name
ORDER BY e.table_name;

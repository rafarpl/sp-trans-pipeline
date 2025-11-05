-- =============================================================================
-- Script 03: Create Materialized Views
-- =============================================================================
-- Descrição: Cria materialized views para otimizar consultas frequentes
-- Ordem de execução: 4/8
-- =============================================================================

\c sptrans_pipeline

-- =============================================================================
-- MV: mv_lines_current (linhas ativas atualmente)
-- =============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS serving.mv_lines_current AS
SELECT 
    line_id,
    line_name,
    COUNT(DISTINCT vehicle_id) as vehicles_active,
    AVG(speed_kmh) as avg_speed_kmh,
    MAX(position_timestamp) as last_update,
    COUNT(*) as total_positions
FROM serving.positions_realtime
WHERE position_timestamp > NOW() - INTERVAL '30 minutes'
GROUP BY line_id, line_name
HAVING COUNT(DISTINCT vehicle_id) > 0
ORDER BY vehicles_active DESC;

-- Índice na MV
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_lines_current_line_id 
    ON serving.mv_lines_current(line_id);

-- =============================================================================
-- MV: mv_fleet_summary (resumo da frota)
-- =============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS serving.mv_fleet_summary AS
SELECT 
    COUNT(DISTINCT vehicle_id) as total_vehicles,
    COUNT(DISTINCT line_id) as total_lines,
    AVG(speed_kmh) as avg_speed,
    COUNT(*) FILTER (WHERE speed_kmh > 5) as vehicles_moving,
    COUNT(*) FILTER (WHERE speed_kmh <= 5) as vehicles_stopped,
    COUNT(*) FILTER (WHERE accessible = true) as vehicles_accessible,
    MAX(position_timestamp) as last_update
FROM serving.positions_realtime
WHERE position_timestamp > NOW() - INTERVAL '30 minutes';

-- =============================================================================
-- MV: mv_hourly_performance_24h (últimas 24h)
-- =============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS serving.mv_hourly_performance_24h AS
SELECT 
    date_trunc('hour', hour_timestamp) as hour,
    COUNT(DISTINCT line_id) as lines_count,
    SUM(vehicles_active) as total_vehicles,
    AVG(avg_speed_kmh) as avg_speed,
    SUM(total_distance_km) as total_distance,
    AVG(data_quality_score) as avg_dq_score
FROM serving.hourly_aggregates
WHERE hour_timestamp > NOW() - INTERVAL '24 hours'
GROUP BY date_trunc('hour', hour_timestamp)
ORDER BY hour DESC;

-- Índice
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_hourly_perf_24h_hour 
    ON serving.mv_hourly_performance_24h(hour);

-- =============================================================================
-- MV: mv_top_lines_by_vehicles (top 20 linhas)
-- =============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS serving.mv_top_lines_by_vehicles AS
SELECT 
    line_id,
    line_name,
    AVG(vehicles_active) as avg_vehicles,
    AVG(avg_speed_kmh) as avg_speed,
    AVG(data_quality_score) as avg_dq_score,
    COUNT(*) as measurements_count
FROM serving.hourly_aggregates
WHERE hour_timestamp > NOW() - INTERVAL '7 days'
GROUP BY line_id, line_name
ORDER BY avg_vehicles DESC
LIMIT 20;

-- Índice
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_top_lines_line_id 
    ON serving.mv_top_lines_by_vehicles(line_id);

-- =============================================================================
-- MV: mv_daily_trends_7d (tendências últimos 7 dias)
-- =============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS serving.mv_daily_trends_7d AS
SELECT 
    date,
    total_vehicles,
    total_lines,
    avg_speed_kmh,
    total_distance_km,
    peak_hour_vehicles,
    peak_hour,
    data_quality_score,
    uptime_percentage
FROM serving.daily_aggregates
WHERE date > CURRENT_DATE - INTERVAL '7 days'
ORDER BY date DESC;

-- Índice
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_daily_trends_date 
    ON serving.mv_daily_trends_7d(date);

-- =============================================================================
-- MV: mv_performance_dashboard (dashboard principal)
-- =============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS serving.mv_performance_dashboard AS
WITH current_hour AS (
    SELECT 
        SUM(vehicles_active) as vehicles_now,
        AVG(avg_speed_kmh) as speed_now,
        COUNT(DISTINCT line_id) as lines_now
    FROM serving.hourly_aggregates
    WHERE hour_timestamp = date_trunc('hour', NOW())
),
last_24h AS (
    SELECT 
        AVG(vehicles_active) as vehicles_24h_avg,
        AVG(avg_speed_kmh) as speed_24h_avg,
        SUM(total_distance_km) as distance_24h_total
    FROM serving.hourly_aggregates
    WHERE hour_timestamp > NOW() - INTERVAL '24 hours'
),
today AS (
    SELECT 
        total_vehicles,
        total_lines,
        peak_hour_vehicles,
        peak_hour,
        data_quality_score
    FROM serving.daily_aggregates
    WHERE date = CURRENT_DATE
)
SELECT 
    c.vehicles_now,
    c.speed_now,
    c.lines_now,
    h.vehicles_24h_avg,
    h.speed_24h_avg,
    h.distance_24h_total,
    t.total_vehicles as vehicles_today_total,
    t.total_lines as lines_today_total,
    t.peak_hour_vehicles,
    t.peak_hour,
    t.data_quality_score as dq_score_today,
    NOW() as last_update
FROM current_hour c
CROSS JOIN last_24h h
LEFT JOIN today t ON true;

-- =============================================================================
-- MV: mv_congestion_hotspots (áreas congestionadas)
-- =============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS serving.mv_congestion_hotspots AS
SELECT 
    line_id,
    line_name,
    avg_speed_kmh,
    congestion_index,
    CASE 
        WHEN avg_speed_kmh < 10 THEN 'Critical'
        WHEN avg_speed_kmh < 15 THEN 'High'
        WHEN avg_speed_kmh < 20 THEN 'Medium'
        ELSE 'Low'
    END as congestion_level,
    period_start,
    period_end
FROM serving.lines_metrics
WHERE analysis_period = 'hourly'
    AND period_start > NOW() - INTERVAL '2 hours'
    AND avg_speed_kmh < 20  -- Apenas linhas com congestionamento
ORDER BY avg_speed_kmh ASC
LIMIT 50;

-- =============================================================================
-- FUNÇÃO: Refresh todas MVs
-- =============================================================================

CREATE OR REPLACE FUNCTION serving.refresh_all_materialized_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY serving.mv_lines_current;
    REFRESH MATERIALIZED VIEW serving.mv_fleet_summary;
    REFRESH MATERIALIZED VIEW CONCURRENTLY serving.mv_hourly_performance_24h;
    REFRESH MATERIALIZED VIEW CONCURRENTLY serving.mv_top_lines_by_vehicles;
    REFRESH MATERIALIZED VIEW CONCURRENTLY serving.mv_daily_trends_7d;
    REFRESH MATERIALIZED VIEW serving.mv_performance_dashboard;
    REFRESH MATERIALIZED VIEW serving.mv_congestion_hotspots;
    
    RAISE NOTICE 'All materialized views refreshed successfully';
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- COMENTÁRIOS
-- =============================================================================

COMMENT ON MATERIALIZED VIEW serving.mv_lines_current IS 
'Linhas ativas atualmente (últimos 30 minutos)';

COMMENT ON MATERIALIZED VIEW serving.mv_fleet_summary IS 
'Resumo geral da frota (atualização: 10 min)';

COMMENT ON MATERIALIZED VIEW serving.mv_hourly_performance_24h IS 
'Performance horária das últimas 24 horas';

COMMENT ON MATERIALIZED VIEW serving.mv_top_lines_by_vehicles IS 
'Top 20 linhas por número de veículos (últimos 7 dias)';

COMMENT ON MATERIALIZED VIEW serving.mv_daily_trends_7d IS 
'Tendências diárias dos últimos 7 dias';

COMMENT ON MATERIALIZED VIEW serving.mv_performance_dashboard IS 
'View agregada para dashboard principal';

COMMENT ON MATERIALIZED VIEW serving.mv_congestion_hotspots IS 
'Linhas com congestionamento (últimas 2 horas)';

COMMENT ON FUNCTION serving.refresh_all_materialized_views() IS 
'Atualiza todas as materialized views do serving layer';

-- =============================================================================
-- PERMISSÕES
-- =============================================================================

GRANT SELECT ON ALL TABLES IN SCHEMA serving TO sptrans_readonly;
GRANT EXECUTE ON FUNCTION serving.refresh_all_materialized_views() TO sptrans_app;

-- =============================================================================
-- VERIFICAÇÃO
-- =============================================================================

SELECT 
    schemaname,
    matviewname,
    ispopulated
FROM pg_matviews
WHERE schemaname = 'serving'
ORDER BY matviewname;

-- =============================================================================
-- FIM DO SCRIPT
-- =============================================================================

\echo '✅ Materialized views criadas!'
\echo '📊 Views:'
\echo '   • mv_lines_current'
\echo '   • mv_fleet_summary'
\echo '   • mv_hourly_performance_24h'
\echo '   • mv_top_lines_by_vehicles'
\echo '   • mv_daily_trends_7d'
\echo '   • mv_performance_dashboard'
\echo '   • mv_congestion_hotspots'
\echo ''
\echo '🔄 Função criada: refresh_all_materialized_views()'
\echo ''
\echo '➡️  Próximo script: 04_indexes.sql'

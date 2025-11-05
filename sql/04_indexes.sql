-- =============================================================================
-- Script 04: Create Indexes
-- =============================================================================
-- Descrição: Cria índices para otimizar queries frequentes
-- Ordem de execução: 5/8
-- =============================================================================

\c sptrans_pipeline

-- =============================================================================
-- ÍNDICES: hourly_aggregates
-- =============================================================================

-- Índice composto para queries por linha e período
CREATE INDEX IF NOT EXISTS idx_hourly_agg_line_timestamp 
    ON serving.hourly_aggregates(line_id, hour_timestamp DESC);

-- Índice para queries por timestamp
CREATE INDEX IF NOT EXISTS idx_hourly_agg_timestamp 
    ON serving.hourly_aggregates(hour_timestamp DESC);

-- Índice para queries por ano/mês (particionamento)
CREATE INDEX IF NOT EXISTS idx_hourly_agg_year_month 
    ON serving.hourly_aggregates(year, month);

-- Índice para busca por nome de linha
CREATE INDEX IF NOT EXISTS idx_hourly_agg_line_name 
    ON serving.hourly_aggregates USING gin(line_name gin_trgm_ops);

-- Índice para filtros de qualidade
CREATE INDEX IF NOT EXISTS idx_hourly_agg_dq_score 
    ON serving.hourly_aggregates(data_quality_score) 
    WHERE data_quality_score < 0.9;

-- =============================================================================
-- ÍNDICES: daily_aggregates
-- =============================================================================

-- PK já cria índice em date
-- Índice para queries por ano/mês
CREATE INDEX IF NOT EXISTS idx_daily_agg_year_month 
    ON serving.daily_aggregates(year, month);

-- Índice para ordenação por data DESC
CREATE INDEX IF NOT EXISTS idx_daily_agg_date_desc 
    ON serving.daily_aggregates(date DESC);

-- =============================================================================
-- ÍNDICES: lines_metrics
-- =============================================================================

-- Índice composto para queries principais
CREATE INDEX IF NOT EXISTS idx_lines_metrics_line_period 
    ON serving.lines_metrics(line_id, analysis_period, period_start DESC);

-- Índice para queries por período
CREATE INDEX IF NOT EXISTS idx_lines_metrics_period_start 
    ON serving.lines_metrics(period_start DESC);

-- Índice para busca por nome
CREATE INDEX IF NOT EXISTS idx_lines_metrics_line_name 
    ON serving.lines_metrics USING gin(line_name gin_trgm_ops);

-- Índice para congestionamento
CREATE INDEX IF NOT EXISTS idx_lines_metrics_congestion 
    ON serving.lines_metrics(congestion_index DESC) 
    WHERE congestion_index > 50;

-- =============================================================================
-- ÍNDICES: positions_realtime
-- =============================================================================

-- Índice composto para queries principais
CREATE INDEX IF NOT EXISTS idx_positions_vehicle_timestamp 
    ON serving.positions_realtime(vehicle_id, position_timestamp DESC);

-- Índice para queries por linha
CREATE INDEX IF NOT EXISTS idx_positions_line_id 
    ON serving.positions_realtime(line_id, position_timestamp DESC);

-- Índice para queries recentes (últimos 30 min)
CREATE INDEX IF NOT EXISTS idx_positions_recent 
    ON serving.positions_realtime(position_timestamp DESC) 
    WHERE position_timestamp > NOW() - INTERVAL '30 minutes';

-- Índice espacial (PostGIS)
CREATE INDEX IF NOT EXISTS idx_positions_geom 
    ON serving.positions_realtime USING GIST(geom);

-- Índice para queries por velocidade
CREATE INDEX IF NOT EXISTS idx_positions_speed 
    ON serving.positions_realtime(speed_kmh) 
    WHERE speed_kmh IS NOT NULL;

-- =============================================================================
-- ÍNDICES: fleet_status
-- =============================================================================

-- PK já cria índice em snapshot_timestamp
-- Índice para queries recentes
CREATE INDEX IF NOT EXISTS idx_fleet_status_recent 
    ON serving.fleet_status(snapshot_timestamp DESC);

-- Índice para system health
CREATE INDEX IF NOT EXISTS idx_fleet_status_health 
    ON serving.fleet_status(system_health, snapshot_timestamp DESC);

-- =============================================================================
-- ÍNDICES: corridors_performance
-- =============================================================================

-- Índice composto principal
CREATE INDEX IF NOT EXISTS idx_corridors_perf_corridor_timestamp 
    ON serving.corridors_performance(corridor_id, analysis_timestamp DESC);

-- Índice para queries por timestamp
CREATE INDEX IF NOT EXISTS idx_corridors_perf_timestamp 
    ON serving.corridors_performance(analysis_timestamp DESC);

-- Índice para congestion level
CREATE INDEX IF NOT EXISTS idx_corridors_perf_congestion 
    ON serving.corridors_performance(congestion_level, analysis_timestamp DESC);

-- =============================================================================
-- ESTATÍSTICAS
-- =============================================================================

-- Atualizar estatísticas após criação dos índices
ANALYZE serving.hourly_aggregates;
ANALYZE serving.daily_aggregates;
ANALYZE serving.lines_metrics;
ANALYZE serving.positions_realtime;
ANALYZE serving.fleet_status;
ANALYZE serving.corridors_performance;

-- =============================================================================
-- VERIFICAÇÃO
-- =============================================================================

-- Listar todos os índices criados
SELECT 
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = 'serving'
ORDER BY tablename, indexname;

-- Tamanho dos índices
SELECT 
    schemaname || '.' || tablename AS table,
    indexrelname AS index_name,
    pg_size_pretty(pg_relation_size(indexrelid)) AS index_size
FROM pg_stat_user_indexes
WHERE schemaname = 'serving'
ORDER BY pg_relation_size(indexrelid) DESC;

-- =============================================================================
-- COMENTÁRIOS
-- =============================================================================

COMMENT ON INDEX serving.idx_hourly_agg_line_timestamp IS 
'Otimiza queries de séries temporais por linha';

COMMENT ON INDEX serving.idx_positions_geom IS 
'Índice espacial para queries geográficas (mapa)';

COMMENT ON INDEX serving.idx_positions_recent IS 
'Índice parcial para dados recentes (últimos 30 min)';

-- =============================================================================
-- FIM DO SCRIPT
-- =============================================================================

\echo '✅ Índices criados com sucesso!'
\echo '📊 Estatísticas:'

SELECT 
    COUNT(*) as total_indexes,
    SUM(pg_relation_size(indexrelid)) / 1024 / 1024 as total_size_mb
FROM pg_stat_user_indexes
WHERE schemaname = 'serving';

\echo ''
\echo '➡️  Próximo script: 05_functions.sql'

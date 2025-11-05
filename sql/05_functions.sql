-- =============================================================================
-- Script 05: Create Functions and Procedures
-- =============================================================================
-- Descrição: Cria funções e procedures para operações comuns
-- Ordem de execução: 6/8
-- =============================================================================

\c sptrans_pipeline

-- =============================================================================
-- FUNÇÃO: Calculate Distance (Haversine)
-- =============================================================================

CREATE OR REPLACE FUNCTION serving.calculate_distance_km(
    lat1 NUMERIC,
    lon1 NUMERIC,
    lat2 NUMERIC,
    lon2 NUMERIC
)
RETURNS NUMERIC AS $$
DECLARE
    R CONSTANT NUMERIC := 6371; -- Raio da Terra em km
    dlat NUMERIC;
    dlon NUMERIC;
    a NUMERIC;
    c NUMERIC;
BEGIN
    -- Converter para radianos
    dlat := radians(lat2 - lat1);
    dlon := radians(lon2 - lon1);
    
    -- Fórmula de Haversine
    a := sin(dlat/2) * sin(dlat/2) + 
         cos(radians(lat1)) * cos(radians(lat2)) * 
         sin(dlon/2) * sin(dlon/2);
    c := 2 * atan2(sqrt(a), sqrt(1-a));
    
    RETURN R * c;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- =============================================================================
-- FUNÇÃO: Get Latest Position
-- =============================================================================

CREATE OR REPLACE FUNCTION serving.get_latest_position(p_vehicle_id VARCHAR)
RETURNS TABLE (
    vehicle_id VARCHAR,
    line_id INTEGER,
    latitude NUMERIC,
    longitude NUMERIC,
    speed_kmh NUMERIC,
    position_timestamp TIMESTAMP
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        p.vehicle_id,
        p.line_id,
        p.latitude,
        p.longitude,
        p.speed_kmh,
        p.position_timestamp
    FROM serving.positions_realtime p
    WHERE p.vehicle_id = p_vehicle_id
    ORDER BY p.position_timestamp DESC
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- FUNÇÃO: Get Active Lines
-- =============================================================================

CREATE OR REPLACE FUNCTION serving.get_active_lines(minutes_ago INTEGER DEFAULT 30)
RETURNS TABLE (
    line_id INTEGER,
    line_name VARCHAR,
    vehicle_count BIGINT,
    avg_speed NUMERIC,
    last_update TIMESTAMP
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        p.line_id,
        p.line_name,
        COUNT(DISTINCT p.vehicle_id) as vehicle_count,
        AVG(p.speed_kmh)::NUMERIC(5,2) as avg_speed,
        MAX(p.position_timestamp) as last_update
    FROM serving.positions_realtime p
    WHERE p.position_timestamp > NOW() - (minutes_ago || ' minutes')::INTERVAL
    GROUP BY p.line_id, p.line_name
    HAVING COUNT(DISTINCT p.vehicle_id) > 0
    ORDER BY vehicle_count DESC;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- FUNÇÃO: Calculate Fleet KPIs
-- =============================================================================

CREATE OR REPLACE FUNCTION serving.calculate_fleet_kpis(for_date DATE DEFAULT CURRENT_DATE)
RETURNS JSON AS $$
DECLARE
    result JSON;
BEGIN
    SELECT json_build_object(
        'date', for_date,
        'total_vehicles', total_vehicles,
        'total_lines', total_lines,
        'avg_speed_kmh', avg_speed_kmh,
        'total_distance_km', total_distance_km,
        'peak_hour_vehicles', peak_hour_vehicles,
        'peak_hour', peak_hour,
        'data_quality_score', data_quality_score
    ) INTO result
    FROM serving.daily_aggregates
    WHERE date = for_date;
    
    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- FUNÇÃO: Get Congested Lines
-- =============================================================================

CREATE OR REPLACE FUNCTION serving.get_congested_lines(
    speed_threshold NUMERIC DEFAULT 15.0,
    hours_ago INTEGER DEFAULT 2
)
RETURNS TABLE (
    line_id INTEGER,
    line_name VARCHAR,
    avg_speed_kmh NUMERIC,
    congestion_index NUMERIC,
    period_start TIMESTAMP
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        lm.line_id,
        lm.line_name,
        lm.avg_speed_kmh,
        lm.congestion_index,
        lm.period_start
    FROM serving.lines_metrics lm
    WHERE lm.avg_speed_kmh < speed_threshold
        AND lm.period_start > NOW() - (hours_ago || ' hours')::INTERVAL
    ORDER BY lm.avg_speed_kmh ASC;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- PROCEDURE: Cleanup Old Data
-- =============================================================================

CREATE OR REPLACE PROCEDURE serving.cleanup_old_data(days_to_keep INTEGER DEFAULT 90)
LANGUAGE plpgsql AS $$
DECLARE
    deleted_count INTEGER;
    cutoff_date DATE;
BEGIN
    cutoff_date := CURRENT_DATE - days_to_keep;
    
    -- Cleanup positions_realtime
    DELETE FROM serving.positions_realtime
    WHERE position_timestamp < cutoff_date;
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RAISE NOTICE 'Deleted % old positions', deleted_count;
    
    -- Cleanup fleet_status
    DELETE FROM serving.fleet_status
    WHERE snapshot_timestamp < cutoff_date;
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RAISE NOTICE 'Deleted % old fleet snapshots', deleted_count;
    
    -- Cleanup daily_aggregates
    DELETE FROM serving.daily_aggregates
    WHERE date < cutoff_date;
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RAISE NOTICE 'Deleted % old daily aggregates', deleted_count;
    
    RAISE NOTICE 'Cleanup completed for data older than %', cutoff_date;
END;
$$;

-- =============================================================================
-- PROCEDURE: Update Statistics
-- =============================================================================

CREATE OR REPLACE PROCEDURE serving.update_table_statistics()
LANGUAGE plpgsql AS $$
BEGIN
    ANALYZE serving.hourly_aggregates;
    ANALYZE serving.daily_aggregates;
    ANALYZE serving.lines_metrics;
    ANALYZE serving.positions_realtime;
    ANALYZE serving.fleet_status;
    ANALYZE serving.corridors_performance;
    
    RAISE NOTICE 'Statistics updated for all serving tables';
END;
$$;

-- =============================================================================
-- FUNÇÃO: Get Performance Report
-- =============================================================================

CREATE OR REPLACE FUNCTION serving.get_performance_report(
    start_date DATE,
    end_date DATE
)
RETURNS TABLE (
    metric VARCHAR,
    value NUMERIC,
    unit VARCHAR
) AS $$
BEGIN
    RETURN QUERY
    SELECT 'Total Vehicles'::VARCHAR, AVG(total_vehicles)::NUMERIC, 'count'::VARCHAR
    FROM serving.daily_aggregates
    WHERE date BETWEEN start_date AND end_date
    UNION ALL
    SELECT 'Avg Speed'::VARCHAR, AVG(avg_speed_kmh)::NUMERIC, 'km/h'::VARCHAR
    FROM serving.daily_aggregates
    WHERE date BETWEEN start_date AND end_date
    UNION ALL
    SELECT 'Total Distance'::VARCHAR, SUM(total_distance_km)::NUMERIC, 'km'::VARCHAR
    FROM serving.daily_aggregates
    WHERE date BETWEEN start_date AND end_date
    UNION ALL
    SELECT 'Avg DQ Score'::VARCHAR, AVG(data_quality_score)::NUMERIC, 'score'::VARCHAR
    FROM serving.daily_aggregates
    WHERE date BETWEEN start_date AND end_date;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- TRIGGER FUNCTION: Update Position Geometry
-- =============================================================================

CREATE OR REPLACE FUNCTION serving.update_position_geom()
RETURNS TRIGGER AS $$
BEGIN
    -- Atualizar coluna geom baseado em lat/lon
    NEW.geom := ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Criar trigger
DROP TRIGGER IF EXISTS trg_update_position_geom ON serving.positions_realtime;
CREATE TRIGGER trg_update_position_geom
    BEFORE INSERT OR UPDATE ON serving.positions_realtime
    FOR EACH ROW
    EXECUTE FUNCTION serving.update_position_geom();

-- =============================================================================
-- PERMISSÕES
-- =============================================================================

GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA serving TO sptrans_app;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA serving TO sptrans_readonly;
GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA serving TO sptrans_app;

-- =============================================================================
-- COMENTÁRIOS
-- =============================================================================

COMMENT ON FUNCTION serving.calculate_distance_km IS 
'Calcula distância entre dois pontos usando fórmula de Haversine';

COMMENT ON FUNCTION serving.get_latest_position IS 
'Retorna última posição conhecida de um veículo';

COMMENT ON FUNCTION serving.get_active_lines IS 
'Retorna linhas ativas nos últimos N minutos';

COMMENT ON FUNCTION serving.calculate_fleet_kpis IS 
'Calcula KPIs da frota para uma data específica (JSON)';

COMMENT ON FUNCTION serving.get_congested_lines IS 
'Retorna linhas com congestionamento (velocidade < threshold)';

COMMENT ON PROCEDURE serving.cleanup_old_data IS 
'Remove dados antigos (> N dias) para economizar espaço';

COMMENT ON PROCEDURE serving.update_table_statistics IS 
'Atualiza estatísticas de todas as tabelas do serving layer';

COMMENT ON FUNCTION serving.get_performance_report IS 
'Gera relatório de performance para período específico';

-- =============================================================================
-- VERIFICAÇÃO
-- =============================================================================

SELECT 
    routine_name,
    routine_type,
    data_type AS return_type
FROM information_schema.routines
WHERE routine_schema = 'serving'
ORDER BY routine_type, routine_name;

-- =============================================================================
-- FIM DO SCRIPT
-- =============================================================================

\echo '✅ Functions e procedures criados!'
\echo '📊 Funções:'
\echo '   • calculate_distance_km'
\echo '   • get_latest_position'
\echo '   • get_active_lines'
\echo '   • calculate_fleet_kpis'
\echo '   • get_congested_lines'
\echo '   • get_performance_report'
\echo ''
\echo '⚙️  Procedures:'
\echo '   • cleanup_old_data'
\echo '   • update_table_statistics'
\echo ''
\echo '🎯 Triggers:'
\echo '   • update_position_geom (PostGIS)'
\echo ''
\echo '➡️  Próximo script: 06_control_tables.sql'

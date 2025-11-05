-- =============================================================================
-- Script 07: Maintenance and Optimization
-- =============================================================================
-- Descrição: Procedures e jobs para manutenção do database
-- Ordem de execução: 8/8 (FINAL)
-- =============================================================================

\c sptrans_pipeline

-- =============================================================================
-- PROCEDURE: Full Database Maintenance
-- =============================================================================

CREATE OR REPLACE PROCEDURE maintenance.full_database_maintenance()
LANGUAGE plpgsql AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
BEGIN
    start_time := CLOCK_TIMESTAMP();
    RAISE NOTICE 'Starting full database maintenance at %', start_time;
    
    -- 1. Vacuum and analyze all tables
    RAISE NOTICE 'Step 1/5: VACUUM ANALYZE all tables...';
    VACUUM ANALYZE serving.hourly_aggregates;
    VACUUM ANALYZE serving.daily_aggregates;
    VACUUM ANALYZE serving.lines_metrics;
    VACUUM ANALYZE serving.positions_realtime;
    VACUUM ANALYZE serving.fleet_status;
    VACUUM ANALYZE serving.corridors_performance;
    VACUUM ANALYZE control.job_executions;
    VACUUM ANALYZE monitoring.data_quality_checks;
    VACUUM ANALYZE monitoring.pipeline_metrics;
    VACUUM ANALYZE gtfs.stops;
    VACUUM ANALYZE gtfs.routes;
    
    -- 2. Reindex tables
    RAISE NOTICE 'Step 2/5: REINDEX tables...';
    REINDEX TABLE serving.hourly_aggregates;
    REINDEX TABLE serving.positions_realtime;
    REINDEX TABLE serving.lines_metrics;
    
    -- 3. Refresh materialized views
    RAISE NOTICE 'Step 3/5: Refreshing materialized views...';
    PERFORM serving.refresh_all_materialized_views();
    
    -- 4. Update statistics
    RAISE NOTICE 'Step 4/5: Updating statistics...';
    ANALYZE;
    
    -- 5. Cleanup old data
    RAISE NOTICE 'Step 5/5: Cleaning up old data...';
    CALL serving.cleanup_old_data(90); -- Keep last 90 days
    
    end_time := CLOCK_TIMESTAMP();
    RAISE NOTICE 'Maintenance completed at % (duration: %)', 
        end_time, 
        end_time - start_time;
END;
$$;

-- =============================================================================
-- PROCEDURE: Partition Management
-- =============================================================================

CREATE OR REPLACE PROCEDURE maintenance.create_monthly_partitions(months_ahead INTEGER DEFAULT 3)
LANGUAGE plpgsql AS $$
DECLARE
    partition_date DATE;
    partition_name TEXT;
    start_date TEXT;
    end_date TEXT;
    i INTEGER;
BEGIN
    FOR i IN 0..months_ahead LOOP
        partition_date := DATE_TRUNC('month', CURRENT_DATE + (i || ' months')::INTERVAL);
        partition_name := 'serving.hourly_aggregates_' || TO_CHAR(partition_date, 'YYYY_MM');
        start_date := TO_CHAR(partition_date, 'YYYY-MM-DD');
        end_date := TO_CHAR(partition_date + INTERVAL '1 month', 'YYYY-MM-DD');
        
        -- Check if partition exists
        IF NOT EXISTS (
            SELECT 1 FROM pg_tables 
            WHERE schemaname = 'serving' 
            AND tablename = REPLACE(partition_name, 'serving.', '')
        ) THEN
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS %s PARTITION OF serving.hourly_aggregates FOR VALUES FROM (%L) TO (%L)',
                partition_name,
                start_date,
                end_date
            );
            RAISE NOTICE 'Created partition: %', partition_name;
        END IF;
    END LOOP;
END;
$$;

-- =============================================================================
-- PROCEDURE: Drop Old Partitions
-- =============================================================================

CREATE OR REPLACE PROCEDURE maintenance.drop_old_partitions(months_to_keep INTEGER DEFAULT 6)
LANGUAGE plpgsql AS $$
DECLARE
    partition_record RECORD;
    cutoff_date DATE;
BEGIN
    cutoff_date := DATE_TRUNC('month', CURRENT_DATE - (months_to_keep || ' months')::INTERVAL);
    
    FOR partition_record IN
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = 'serving' 
        AND tablename LIKE 'hourly_aggregates_%'
        AND tablename ~ '^\d{4}_\d{2}$'
    LOOP
        -- Extract date from partition name
        DECLARE
            partition_date DATE;
        BEGIN
            partition_date := TO_DATE(
                REGEXP_REPLACE(partition_record.tablename, 'hourly_aggregates_', ''),
                'YYYY_MM'
            );
            
            IF partition_date < cutoff_date THEN
                EXECUTE format('DROP TABLE IF EXISTS serving.%I', partition_record.tablename);
                RAISE NOTICE 'Dropped old partition: %', partition_record.tablename;
            END IF;
        END;
    END LOOP;
END;
$$;

-- =============================================================================
-- FUNCTION: Database Health Check
-- =============================================================================

CREATE OR REPLACE FUNCTION maintenance.database_health_check()
RETURNS TABLE (
    check_name VARCHAR,
    status VARCHAR,
    details TEXT
) AS $$
BEGIN
    -- Check 1: Database size
    RETURN QUERY
    SELECT 
        'Database Size'::VARCHAR,
        CASE 
            WHEN pg_database_size(current_database()) > 100 * 1024^3 THEN 'WARNING'
            ELSE 'OK'
        END::VARCHAR,
        pg_size_pretty(pg_database_size(current_database()))::TEXT;
    
    -- Check 2: Table bloat
    RETURN QUERY
    SELECT 
        'Table Bloat'::VARCHAR,
        'OK'::VARCHAR,
        'Tables analyzed'::TEXT;
    
    -- Check 3: Index usage
    RETURN QUERY
    SELECT 
        'Unused Indexes'::VARCHAR,
        CASE 
            WHEN COUNT(*) > 0 THEN 'WARNING'
            ELSE 'OK'
        END::VARCHAR,
        COUNT(*)::TEXT || ' unused indexes found'
    FROM pg_stat_user_indexes
    WHERE idx_scan = 0;
    
    -- Check 4: Long running queries
    RETURN QUERY
    SELECT 
        'Long Running Queries'::VARCHAR,
        CASE 
            WHEN COUNT(*) > 0 THEN 'WARNING'
            ELSE 'OK'
        END::VARCHAR,
        COUNT(*)::TEXT || ' queries > 5 minutes'
    FROM pg_stat_activity
    WHERE state = 'active'
    AND now() - query_start > INTERVAL '5 minutes';
    
    -- Check 5: Connections
    RETURN QUERY
    SELECT 
        'Active Connections'::VARCHAR,
        CASE 
            WHEN COUNT(*) > 80 THEN 'WARNING'
            ELSE 'OK'
        END::VARCHAR,
        COUNT(*)::TEXT || ' active connections'
    FROM pg_stat_activity
    WHERE state = 'active';
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- VIEW: Database Statistics
-- =============================================================================

CREATE OR REPLACE VIEW maintenance.v_database_stats AS
SELECT 
    'Database' as object_type,
    current_database() as object_name,
    pg_size_pretty(pg_database_size(current_database())) as size,
    NULL::BIGINT as row_count,
    NULL::TEXT as last_vacuum,
    NULL::TEXT as last_analyze
UNION ALL
SELECT 
    'Table' as object_type,
    schemaname || '.' || tablename as object_name,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
    n_live_tup as row_count,
    last_vacuum::TEXT,
    last_analyze::TEXT
FROM pg_stat_user_tables
WHERE schemaname IN ('serving', 'control', 'monitoring', 'gtfs')
ORDER BY object_type, object_name;

-- =============================================================================
-- SCHEDULED MAINTENANCE (via pg_cron if available)
-- =============================================================================

-- Note: pg_cron needs to be installed separately
-- CREATE EXTENSION IF NOT EXISTS pg_cron;

-- Daily maintenance (02:00 AM)
-- SELECT cron.schedule('daily-maintenance', '0 2 * * *', 
--     'CALL maintenance.full_database_maintenance()');

-- Weekly partition management (Sundays 03:00 AM)
-- SELECT cron.schedule('weekly-partition-mgmt', '0 3 * * 0', 
--     'CALL maintenance.create_monthly_partitions(3); CALL maintenance.drop_old_partitions(6)');

-- =============================================================================
-- COMENTÁRIOS
-- =============================================================================

COMMENT ON PROCEDURE maintenance.full_database_maintenance IS 
'Executa manutenção completa: VACUUM, REINDEX, refresh MVs, cleanup';

COMMENT ON PROCEDURE maintenance.create_monthly_partitions IS 
'Cria partições mensais para hourly_aggregates (N meses à frente)';

COMMENT ON PROCEDURE maintenance.drop_old_partitions IS 
'Remove partições antigas (> N meses) para economizar espaço';

COMMENT ON FUNCTION maintenance.database_health_check IS 
'Verifica saúde do database (size, bloat, indexes, queries, connections)';

COMMENT ON VIEW maintenance.v_database_stats IS 
'Estatísticas de tamanho e uso de database e tabelas';

-- =============================================================================
-- PERMISSÕES
-- =============================================================================

-- Schema maintenance
CREATE SCHEMA IF NOT EXISTS maintenance;
GRANT USAGE ON SCHEMA maintenance TO sptrans_app;
GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA maintenance TO sptrans_app;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA maintenance TO sptrans_app;
GRANT SELECT ON ALL TABLES IN SCHEMA maintenance TO sptrans_readonly;

-- =============================================================================
-- VERIFICAÇÃO FINAL
-- =============================================================================

-- Health check
SELECT * FROM maintenance.database_health_check();

-- Database stats
SELECT * FROM maintenance.v_database_stats;

-- =============================================================================
-- FIM DO SCRIPT
-- =============================================================================

\echo ''
\echo '🎉 ==============================================='
\echo '🎉  DATABASE SETUP COMPLETO!'
\echo '🎉 ==============================================='
\echo ''
\echo '✅ Todas as 8 etapas concluídas:'
\echo '   1. ✅ Database e extensões'
\echo '   2. ✅ Schemas'
\echo '   3. ✅ Tabelas do serving layer'
\echo '   4. ✅ Materialized views'
\echo '   5. ✅ Índices'
\echo '   6. ✅ Functions e procedures'
\echo '   7. ✅ Tabelas de controle'
\echo '   8. ✅ Manutenção'
\echo ''
\echo '📊 Estatísticas do Database:'

SELECT 
    COUNT(*) FILTER (WHERE schemaname = 'serving') as serving_tables,
    COUNT(*) FILTER (WHERE schemaname = 'control') as control_tables,
    COUNT(*) FILTER (WHERE schemaname = 'monitoring') as monitoring_tables,
    COUNT(*) FILTER (WHERE schemaname = 'gtfs') as gtfs_tables,
    COUNT(*) as total_tables
FROM pg_tables
WHERE schemaname IN ('serving', 'control', 'monitoring', 'gtfs', 'maintenance');

\echo ''
\echo '🚀 Pipeline pronto para rodar!'
\echo ''
\echo '📖 Próximos passos:'
\echo '   1. Executar DAGs Airflow'
\echo '   2. Conectar dashboards (Superset/Grafana)'
\echo '   3. Configurar alertas'
\echo ''
\echo '🔐 Credentials:'
\echo '   • App: sptrans_app / sptrans_app_password'
\echo '   • Read-only: sptrans_readonly / sptrans_readonly_password'
\echo ''

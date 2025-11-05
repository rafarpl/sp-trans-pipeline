-- =============================================================================
-- Script 06: Create Control and Monitoring Tables
-- =============================================================================
-- Descrição: Cria tabelas para controle do pipeline e monitoramento
-- Ordem de execução: 7/8
-- =============================================================================

\c sptrans_pipeline

-- =============================================================================
-- SCHEMA CONTROL: Job Execution Control
-- =============================================================================

CREATE TABLE IF NOT EXISTS control.job_executions (
    execution_id SERIAL PRIMARY KEY,
    job_name VARCHAR(100) NOT NULL,
    job_type VARCHAR(50) NOT NULL, -- 'ingestion', 'transformation', 'aggregation', 'load'
    stage VARCHAR(20) NOT NULL, -- 'bronze', 'silver', 'gold', 'serving'
    
    -- Execution details
    start_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds INTEGER GENERATED ALWAYS AS (EXTRACT(EPOCH FROM (end_time - start_time))) STORED,
    
    -- Status
    status VARCHAR(20) NOT NULL, -- 'running', 'success', 'failed', 'timeout'
    error_message TEXT,
    
    -- Metrics
    input_records BIGINT,
    output_records BIGINT,
    records_processed BIGINT,
    bytes_processed BIGINT,
    
    -- Context
    airflow_dag_id VARCHAR(100),
    airflow_task_id VARCHAR(100),
    airflow_execution_date TIMESTAMP,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_job_exec_job_name ON control.job_executions(job_name, start_time DESC);
CREATE INDEX idx_job_exec_status ON control.job_executions(status, start_time DESC);
CREATE INDEX idx_job_exec_start_time ON control.job_executions(start_time DESC);

-- =============================================================================
-- SCHEMA MONITORING: Data Quality Checks
-- =============================================================================

CREATE TABLE IF NOT EXISTS monitoring.data_quality_checks (
    check_id SERIAL PRIMARY KEY,
    check_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Context
    stage VARCHAR(20) NOT NULL,
    table_name VARCHAR(100),
    check_name VARCHAR(100) NOT NULL,
    
    -- Results
    status VARCHAR(20) NOT NULL, -- 'passed', 'failed', 'warning'
    score NUMERIC(5,2), -- 0-100
    
    -- Metrics
    total_records BIGINT,
    valid_records BIGINT,
    invalid_records BIGINT,
    null_records BIGINT,
    duplicate_records BIGINT,
    
    -- Details
    validation_rules JSONB,
    error_details JSONB,
    
    -- Metadata
    execution_id INTEGER REFERENCES control.job_executions(execution_id)
);

CREATE INDEX idx_dq_checks_timestamp ON monitoring.data_quality_checks(check_timestamp DESC);
CREATE INDEX idx_dq_checks_stage ON monitoring.data_quality_checks(stage, check_timestamp DESC);
CREATE INDEX idx_dq_checks_status ON monitoring.data_quality_checks(status, check_timestamp DESC);

-- =============================================================================
-- SCHEMA MONITORING: Pipeline Metrics
-- =============================================================================

CREATE TABLE IF NOT EXISTS monitoring.pipeline_metrics (
    metric_id SERIAL PRIMARY KEY,
    metric_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Metric identification
    metric_name VARCHAR(100) NOT NULL,
    metric_type VARCHAR(50) NOT NULL, -- 'counter', 'gauge', 'histogram'
    stage VARCHAR(20),
    
    -- Value
    metric_value NUMERIC,
    metric_unit VARCHAR(20),
    
    -- Labels (Prometheus-style)
    labels JSONB,
    
    -- Context
    execution_id INTEGER REFERENCES control.job_executions(execution_id)
);

CREATE INDEX idx_pipeline_metrics_timestamp ON monitoring.pipeline_metrics(metric_timestamp DESC);
CREATE INDEX idx_pipeline_metrics_name ON monitoring.pipeline_metrics(metric_name, metric_timestamp DESC);
CREATE INDEX idx_pipeline_metrics_labels ON monitoring.pipeline_metrics USING gin(labels);

-- =============================================================================
-- SCHEMA MONITORING: Alerts
-- =============================================================================

CREATE TABLE IF NOT EXISTS monitoring.alerts (
    alert_id SERIAL PRIMARY KEY,
    alert_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Alert details
    alert_level VARCHAR(20) NOT NULL, -- 'info', 'warning', 'error', 'critical'
    alert_type VARCHAR(50) NOT NULL,
    alert_message TEXT NOT NULL,
    
    -- Context
    stage VARCHAR(20),
    job_name VARCHAR(100),
    
    -- Details
    alert_details JSONB,
    
    -- Resolution
    resolved BOOLEAN DEFAULT FALSE,
    resolved_at TIMESTAMP,
    resolved_by VARCHAR(100),
    resolution_notes TEXT,
    
    -- Notification
    notification_sent BOOLEAN DEFAULT FALSE,
    notification_channel VARCHAR(50)
);

CREATE INDEX idx_alerts_timestamp ON monitoring.alerts(alert_timestamp DESC);
CREATE INDEX idx_alerts_level ON monitoring.alerts(alert_level, alert_timestamp DESC);
CREATE INDEX idx_alerts_resolved ON monitoring.alerts(resolved, alert_timestamp DESC);

-- =============================================================================
-- SCHEMA GTFS: Static Data Tables
-- =============================================================================

CREATE TABLE IF NOT EXISTS gtfs.stops (
    stop_id VARCHAR(50) PRIMARY KEY,
    stop_code VARCHAR(50),
    stop_name VARCHAR(255) NOT NULL,
    stop_lat NUMERIC(10,8) NOT NULL,
    stop_lon NUMERIC(11,8) NOT NULL,
    stop_desc TEXT,
    zone_id VARCHAR(50),
    stop_url VARCHAR(500),
    location_type INTEGER,
    parent_station VARCHAR(50),
    wheelchair_boarding INTEGER,
    geom GEOMETRY(POINT, 4326),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_gtfs_stops_geom ON gtfs.stops USING GIST(geom);
CREATE INDEX idx_gtfs_stops_name ON gtfs.stops USING gin(stop_name gin_trgm_ops);

CREATE TABLE IF NOT EXISTS gtfs.routes (
    route_id VARCHAR(50) PRIMARY KEY,
    agency_id VARCHAR(50),
    route_short_name VARCHAR(50),
    route_long_name VARCHAR(255) NOT NULL,
    route_desc TEXT,
    route_type INTEGER NOT NULL,
    route_url VARCHAR(500),
    route_color VARCHAR(10),
    route_text_color VARCHAR(10),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_gtfs_routes_short_name ON gtfs.routes(route_short_name);

-- =============================================================================
-- VIEWS: Monitoring Dashboard
-- =============================================================================

CREATE OR REPLACE VIEW monitoring.v_recent_jobs AS
SELECT 
    job_name,
    job_type,
    stage,
    status,
    start_time,
    end_time,
    duration_seconds,
    input_records,
    output_records,
    error_message
FROM control.job_executions
ORDER BY start_time DESC
LIMIT 100;

CREATE OR REPLACE VIEW monitoring.v_data_quality_summary AS
SELECT 
    stage,
    check_name,
    status,
    AVG(score) as avg_score,
    COUNT(*) as check_count,
    MAX(check_timestamp) as last_check
FROM monitoring.data_quality_checks
WHERE check_timestamp > NOW() - INTERVAL '24 hours'
GROUP BY stage, check_name, status
ORDER BY stage, check_name;

CREATE OR REPLACE VIEW monitoring.v_active_alerts AS
SELECT 
    alert_timestamp,
    alert_level,
    alert_type,
    alert_message,
    stage,
    job_name
FROM monitoring.alerts
WHERE NOT resolved
ORDER BY alert_timestamp DESC;

-- =============================================================================
-- COMENTÁRIOS
-- =============================================================================

COMMENT ON TABLE control.job_executions IS 'Registro de execuções de jobs do pipeline';
COMMENT ON TABLE monitoring.data_quality_checks IS 'Resultados de checks de qualidade de dados';
COMMENT ON TABLE monitoring.pipeline_metrics IS 'Métricas coletadas durante execução do pipeline';
COMMENT ON TABLE monitoring.alerts IS 'Alertas gerados pelo sistema de monitoramento';
COMMENT ON TABLE gtfs.stops IS 'Paradas de ônibus (dados GTFS estáticos)';
COMMENT ON TABLE gtfs.routes IS 'Rotas/linhas de ônibus (dados GTFS estáticos)';

-- =============================================================================
-- PERMISSÕES
-- =============================================================================

GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA control TO sptrans_app;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA monitoring TO sptrans_app;
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA gtfs TO sptrans_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA control TO sptrans_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA monitoring TO sptrans_app;

GRANT SELECT ON ALL TABLES IN SCHEMA control TO sptrans_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA monitoring TO sptrans_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA gtfs TO sptrans_readonly;

-- =============================================================================
-- FIM DO SCRIPT
-- =============================================================================

\echo '✅ Tabelas de controle e monitoramento criadas!'
\echo '📊 Control Schema:'
\echo '   • job_executions'
\echo ''
\echo '📊 Monitoring Schema:'
\echo '   • data_quality_checks'
\echo '   • pipeline_metrics'
\echo '   • alerts'
\echo ''
\echo '📊 GTFS Schema:'
\echo '   • stops'
\echo '   • routes'
\echo ''
\echo '➡️  Próximo script: 07_maintenance.sql (FINAL)'

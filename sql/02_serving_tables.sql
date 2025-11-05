-- =============================================================================
-- Script 02: Create Serving Tables
-- =============================================================================
-- Descrição: Cria tabelas do serving layer (Gold → PostgreSQL)
-- Ordem de execução: 3/8
-- =============================================================================

\c sptrans_pipeline

-- =============================================================================
-- TABELA: hourly_aggregates
-- =============================================================================

CREATE TABLE IF NOT EXISTS serving.hourly_aggregates (
    -- Chaves
    line_id INTEGER NOT NULL,
    hour_timestamp TIMESTAMP NOT NULL,
    
    -- Dimensões
    line_name VARCHAR(255),
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    
    -- Métricas de frota
    vehicles_active INTEGER,
    vehicles_accessible INTEGER,
    
    -- Métricas de velocidade
    avg_speed_kmh NUMERIC(5,2),
    min_speed_kmh NUMERIC(5,2),
    max_speed_kmh NUMERIC(5,2),
    std_speed_kmh NUMERIC(5,2),
    
    -- Métricas de distância
    total_distance_km NUMERIC(10,2),
    avg_distance_per_vehicle_km NUMERIC(8,2),
    
    -- Qualidade de dados
    data_quality_score NUMERIC(3,2),
    total_records BIGINT,
    valid_records BIGINT,
    
    -- Metadados
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    PRIMARY KEY (line_id, hour_timestamp)
) PARTITION BY RANGE (hour_timestamp);

-- Criar partições para últimos 3 meses
CREATE TABLE IF NOT EXISTS serving.hourly_aggregates_2024_09 
    PARTITION OF serving.hourly_aggregates 
    FOR VALUES FROM ('2024-09-01') TO ('2024-10-01');

CREATE TABLE IF NOT EXISTS serving.hourly_aggregates_2024_10 
    PARTITION OF serving.hourly_aggregates 
    FOR VALUES FROM ('2024-10-01') TO ('2024-11-01');

CREATE TABLE IF NOT EXISTS serving.hourly_aggregates_2024_11 
    PARTITION OF serving.hourly_aggregates 
    FOR VALUES FROM ('2024-11-01') TO ('2024-12-01');

-- =============================================================================
-- TABELA: daily_aggregates
-- =============================================================================

CREATE TABLE IF NOT EXISTS serving.daily_aggregates (
    -- Chaves
    date DATE NOT NULL PRIMARY KEY,
    
    -- Dimensões
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    
    -- Métricas gerais
    total_vehicles INTEGER,
    total_lines INTEGER,
    total_trips BIGINT,
    
    -- Performance
    avg_speed_kmh NUMERIC(5,2),
    total_distance_km NUMERIC(12,2),
    
    -- Operação
    peak_hour_vehicles INTEGER,
    peak_hour INTEGER,
    off_peak_avg_vehicles NUMERIC(8,2),
    
    -- Qualidade
    data_quality_score NUMERIC(3,2),
    uptime_percentage NUMERIC(5,2),
    
    -- Metadados
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- TABELA: lines_metrics
-- =============================================================================

CREATE TABLE IF NOT EXISTS serving.lines_metrics (
    -- Chaves
    line_id INTEGER NOT NULL,
    analysis_period VARCHAR(50) NOT NULL,
    period_start TIMESTAMP NOT NULL,
    period_end TIMESTAMP NOT NULL,
    
    -- Dimensões
    line_name VARCHAR(255),
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    
    -- KPIs operacionais
    avg_headway_minutes NUMERIC(6,2),
    std_headway_minutes NUMERIC(6,2),
    reliability_score NUMERIC(5,2),
    
    -- KPIs de velocidade
    avg_speed_kmh NUMERIC(5,2),
    congestion_index NUMERIC(5,2),
    
    -- KPIs de frota
    avg_vehicles_operating NUMERIC(8,2),
    utilization_rate NUMERIC(4,3),
    
    -- Metadados
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    PRIMARY KEY (line_id, analysis_period, period_start)
);

-- =============================================================================
-- TABELA: positions_realtime (últimas posições)
-- =============================================================================

CREATE TABLE IF NOT EXISTS serving.positions_realtime (
    -- Chaves
    vehicle_id VARCHAR(50) NOT NULL,
    position_timestamp TIMESTAMP NOT NULL,
    
    -- Identificadores
    line_id INTEGER,
    line_name VARCHAR(255),
    line_direction INTEGER,
    
    -- Posição
    latitude NUMERIC(10,8) NOT NULL,
    longitude NUMERIC(11,8) NOT NULL,
    geom GEOMETRY(POINT, 4326), -- PostGIS
    
    -- Características
    accessible BOOLEAN,
    speed_kmh NUMERIC(5,2),
    
    -- Metadados
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    PRIMARY KEY (vehicle_id, position_timestamp)
);

-- =============================================================================
-- TABELA: fleet_status (resumo da frota)
-- =============================================================================

CREATE TABLE IF NOT EXISTS serving.fleet_status (
    -- Snapshot timestamp
    snapshot_timestamp TIMESTAMP NOT NULL PRIMARY KEY,
    
    -- Métricas totais
    total_vehicles_active INTEGER,
    total_vehicles_accessible INTEGER,
    total_lines_operating INTEGER,
    
    -- Por tipo de veículo
    vehicles_in_motion INTEGER,
    vehicles_stopped INTEGER,
    
    -- Velocidades
    avg_speed_all NUMERIC(5,2),
    avg_speed_in_motion NUMERIC(5,2),
    
    -- Áreas
    vehicles_downtown INTEGER,
    vehicles_suburbs INTEGER,
    
    -- Status do sistema
    system_health VARCHAR(20), -- healthy, degraded, critical
    data_quality_score NUMERIC(3,2),
    
    -- Metadados
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- TABELA: corridors_performance
-- =============================================================================

CREATE TABLE IF NOT EXISTS serving.corridors_performance (
    -- Chaves
    corridor_id INTEGER NOT NULL,
    analysis_timestamp TIMESTAMP NOT NULL,
    
    -- Identificação
    corridor_name VARCHAR(255),
    
    -- Métricas
    avg_speed_kmh NUMERIC(5,2),
    vehicle_count INTEGER,
    congestion_level VARCHAR(20), -- low, medium, high, critical
    
    -- Metadados
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (corridor_id, analysis_timestamp)
);

-- =============================================================================
-- COMENTÁRIOS
-- =============================================================================

COMMENT ON TABLE serving.hourly_aggregates IS 
'Agregações horárias por linha - métricas de frota, velocidade e distância';

COMMENT ON TABLE serving.daily_aggregates IS 
'Sumarização diária do sistema completo';

COMMENT ON TABLE serving.lines_metrics IS 
'Métricas de performance e KPIs por linha';

COMMENT ON TABLE serving.positions_realtime IS 
'Últimas posições conhecidas dos veículos (para mapa em tempo real)';

COMMENT ON TABLE serving.fleet_status IS 
'Snapshot do status da frota em intervalos regulares';

COMMENT ON TABLE serving.corridors_performance IS 
'Performance dos corredores de ônibus';

-- =============================================================================
-- VERIFICAÇÃO
-- =============================================================================

SELECT 
    schemaname,
    tablename,
    tableowner
FROM pg_tables
WHERE schemaname = 'serving'
ORDER BY tablename;

-- =============================================================================
-- FIM DO SCRIPT
-- =============================================================================

\echo '✅ Tabelas do serving layer criadas!'
\echo '📊 Tabelas:'
\echo '   • hourly_aggregates (particionada)'
\echo '   • daily_aggregates'
\echo '   • lines_metrics'
\echo '   • positions_realtime'
\echo '   • fleet_status'
\echo '   • corridors_performance'
\echo ''
\echo '➡️  Próximo script: 03_materialized_views.sql'

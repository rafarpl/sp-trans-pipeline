-- ============================================================================
-- SCHEMA SERVING - Tabelas otimizadas para Grafana
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS serving;

-- Limpar tabelas existentes (se necessário)
DROP TABLE IF EXISTS serving.kpi_realtime CASCADE;
DROP TABLE IF EXISTS serving.kpi_by_line CASCADE;
DROP TABLE IF EXISTS serving.kpi_quality CASCADE;
DROP TABLE IF EXISTS serving.vehicle_positions_latest CASCADE;

-- ============================================================================
-- TABELA 1: KPIs em Tempo Real (atualizada a cada 2-3 min)
-- ============================================================================
CREATE TABLE serving.kpi_realtime (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    
    -- KPI 1: Ônibus em Circulação
    total_vehicles INTEGER NOT NULL,
    total_lines INTEGER NOT NULL,
    
    -- KPI 2: Taxa de Atualização
    stale_vehicles INTEGER DEFAULT 0,
    stale_percentage DECIMAL(5,2) DEFAULT 0,
    
    -- KPI 4: Cobertura
    coverage_percentage DECIMAL(5,2) DEFAULT 0,
    
    -- KPI 3: Tempo Médio Entre Mensagens
    avg_update_interval_seconds INTEGER DEFAULT 0,
    
    -- Metadados
    data_source VARCHAR(20) DEFAULT 'api',
    records_processed INTEGER DEFAULT 0,
    
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_kpi_realtime_timestamp ON serving.kpi_realtime(timestamp DESC);

-- ============================================================================
-- TABELA 2: KPIs por Linha (agregado por linha e janela temporal)
-- ============================================================================
CREATE TABLE serving.kpi_by_line (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    line_id VARCHAR(20) NOT NULL,
    
    -- Métricas de frota
    total_vehicles INTEGER NOT NULL,
    
    -- Métricas de velocidade
    avg_speed DECIMAL(5,2),
    max_speed DECIMAL(5,2),
    min_speed DECIMAL(5,2),
    
    -- Distribuição de velocidade
    vehicles_0_20kmh INTEGER DEFAULT 0,
    vehicles_20_40kmh INTEGER DEFAULT 0,
    vehicles_40_60kmh INTEGER DEFAULT 0,
    vehicles_60plus_kmh INTEGER DEFAULT 0,
    
    -- Cobertura espacial (opcional - para análise avançada)
    coverage_area_km2 DECIMAL(10,2),
    
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_kpi_line_timestamp ON serving.kpi_by_line(timestamp DESC, line_id);
CREATE INDEX idx_kpi_line_id ON serving.kpi_by_line(line_id);

-- ============================================================================
-- TABELA 3: Qualidade de Dados (métricas do pipeline)
-- ============================================================================
CREATE TABLE serving.kpi_quality (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    
    -- Pipeline Health
    pipeline_status VARCHAR(20) DEFAULT 'running',
    execution_duration_seconds INTEGER,
    
    -- Data Quality
    records_ingested INTEGER DEFAULT 0,
    records_valid INTEGER DEFAULT 0,
    records_invalid INTEGER DEFAULT 0,
    validation_rate DECIMAL(5,2),
    
    -- Staleness
    vehicles_stale INTEGER DEFAULT 0,
    staleness_percentage DECIMAL(5,2),
    
    -- MTBF / MTTR (simplificado)
    avg_downtime_minutes DECIMAL(10,2) DEFAULT 0,
    
    -- Alertas
    alerts_triggered INTEGER DEFAULT 0,
    alert_details TEXT,
    
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_kpi_quality_timestamp ON serving.kpi_quality(timestamp DESC);

-- ============================================================================
-- TABELA 4: Posições Mais Recentes (para mapa)
-- ============================================================================
CREATE TABLE serving.vehicle_positions_latest (
    vehicle_id VARCHAR(50) PRIMARY KEY,
    line_id VARCHAR(20) NOT NULL,
    latitude DECIMAL(10,7) NOT NULL,
    longitude DECIMAL(10,7) NOT NULL,
    speed DECIMAL(5,2),
    timestamp TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_positions_latest_timestamp ON serving.vehicle_positions_latest(timestamp DESC);
CREATE INDEX idx_positions_latest_line ON serving.vehicle_positions_latest(line_id);
CREATE INDEX idx_positions_latest_location ON serving.vehicle_positions_latest(latitude, longitude);

-- ============================================================================
-- TABELA 5: Histórico para Séries Temporais (últimas 48h)
-- ============================================================================
CREATE TABLE serving.kpi_timeseries (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    metric_name VARCHAR(50) NOT NULL,
    metric_value DECIMAL(10,2) NOT NULL,
    line_id VARCHAR(20),  -- NULL = métrica global
    
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_kpi_timeseries_timestamp ON serving.kpi_timeseries(timestamp DESC);
CREATE INDEX idx_kpi_timeseries_metric ON serving.kpi_timeseries(metric_name, timestamp DESC);
CREATE INDEX idx_kpi_timeseries_line ON serving.kpi_timeseries(line_id, timestamp DESC);

-- ============================================================================
-- FUNÇÃO: Limpeza Automática (manter últimas 48h)
-- ============================================================================
CREATE OR REPLACE FUNCTION serving.cleanup_old_data() 
RETURNS void AS $$
BEGIN
    DELETE FROM serving.kpi_realtime WHERE timestamp < NOW() - INTERVAL '30 days';
    DELETE FROM serving.kpi_by_line WHERE timestamp < NOW() - INTERVAL '30 days';
    DELETE FROM serving.kpi_quality WHERE timestamp < NOW() - INTERVAL '30 days';
    DELETE FROM serving.vehicle_positions_latest WHERE timestamp < NOW() - INTERVAL '1 hour';  -- Manter 1 hora
    DELETE FROM serving.kpi_timeseries WHERE timestamp < NOW() - INTERVAL '30 days';
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- VIEW: Dashboard Summary (última snapshot)
-- ============================================================================
CREATE OR REPLACE VIEW serving.dashboard_summary AS
SELECT 
    r.timestamp,
    r.total_vehicles,
    r.total_lines,
    r.coverage_percentage,
    r.stale_percentage,
    r.avg_update_interval_seconds,
    q.pipeline_status,
    q.validation_rate,
    r.data_source
FROM serving.kpi_realtime r
LEFT JOIN serving.kpi_quality q ON q.timestamp = (
    SELECT MAX(timestamp) FROM serving.kpi_quality
)
WHERE r.timestamp = (SELECT MAX(timestamp) FROM serving.kpi_realtime)
LIMIT 1;

-- ============================================================================
-- Grants (permissões)
-- ============================================================================
GRANT ALL ON SCHEMA serving TO test_user;
GRANT ALL ON ALL TABLES IN SCHEMA serving TO test_user;
GRANT ALL ON ALL SEQUENCES IN SCHEMA serving TO test_user;

-- ============================================================================
-- Dados iniciais (seed) para testar Grafana
-- ============================================================================
INSERT INTO serving.kpi_realtime (timestamp, total_vehicles, total_lines, coverage_percentage, data_source)
VALUES (NOW(), 0, 0, 0, 'initializing');

INSERT INTO serving.kpi_quality (timestamp, pipeline_status, records_ingested, records_valid, validation_rate)
VALUES (NOW(), 'starting', 0, 0, 0);

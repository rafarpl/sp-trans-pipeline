-- =============================================================================
-- Script 01: Create Schemas
-- =============================================================================
-- Descrição: Cria schemas para organização lógica das tabelas
-- Ordem de execução: 2/8
-- =============================================================================

\c sptrans_pipeline

-- =============================================================================
-- SCHEMAS
-- =============================================================================

-- Schema para serving layer (dados para consumo/dashboards)
CREATE SCHEMA IF NOT EXISTS serving;

-- Schema para controle e metadados
CREATE SCHEMA IF NOT EXISTS control;

-- Schema para dados GTFS (dados estáticos)
CREATE SCHEMA IF NOT EXISTS gtfs;

-- Schema para monitoramento
CREATE SCHEMA IF NOT EXISTS monitoring;

-- =============================================================================
-- PERMISSÕES
-- =============================================================================

-- Grant usage nos schemas para role da aplicação
GRANT USAGE ON SCHEMA serving TO sptrans_app;
GRANT USAGE ON SCHEMA control TO sptrans_app;
GRANT USAGE ON SCHEMA gtfs TO sptrans_app;
GRANT USAGE ON SCHEMA monitoring TO sptrans_app;

-- Grant ALL no serving schema para app
GRANT ALL ON SCHEMA serving TO sptrans_app;
GRANT ALL ON ALL TABLES IN SCHEMA serving TO sptrans_app;
GRANT ALL ON ALL SEQUENCES IN SCHEMA serving TO sptrans_app;

-- Grant usage e select no serving para readonly
GRANT USAGE ON SCHEMA serving TO sptrans_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA serving TO sptrans_readonly;

-- Default privileges (para tabelas futuras)
ALTER DEFAULT PRIVILEGES IN SCHEMA serving
    GRANT SELECT ON TABLES TO sptrans_readonly;

ALTER DEFAULT PRIVILEGES IN SCHEMA serving
    GRANT ALL ON TABLES TO sptrans_app;

-- =============================================================================
-- COMENTÁRIOS
-- =============================================================================

COMMENT ON SCHEMA serving IS 
'Schema para dados agregados e otimizados para consumo (dashboards, APIs)';

COMMENT ON SCHEMA control IS 
'Schema para tabelas de controle, logs de execução e metadados do pipeline';

COMMENT ON SCHEMA gtfs IS 
'Schema para dados estáticos GTFS (linhas, paradas, viagens, horários)';

COMMENT ON SCHEMA monitoring IS 
'Schema para métricas de monitoramento e data quality';

-- =============================================================================
-- VERIFICAÇÃO
-- =============================================================================

-- Listar schemas criados
SELECT 
    schema_name,
    schema_owner
FROM information_schema.schemata
WHERE schema_name IN ('serving', 'control', 'gtfs', 'monitoring')
ORDER BY schema_name;

-- =============================================================================
-- FIM DO SCRIPT
-- =============================================================================

\echo '✅ Schemas criados com sucesso!'
\echo '📊 Schemas:'
\echo '   • serving - Dados para dashboards'
\echo '   • control - Controle do pipeline'
\echo '   • gtfs - Dados estáticos'
\echo '   • monitoring - Métricas e DQ'
\echo ''
\echo '➡️  Próximo script: 02_serving_tables.sql'

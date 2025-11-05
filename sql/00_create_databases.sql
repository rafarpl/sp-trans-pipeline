-- =============================================================================
-- Script 00: Create Databases and Extensions
-- =============================================================================
-- Descrição: Cria databases e instala extensões necessárias
-- Autor: SPTrans Pipeline Team
-- Data: 2024-11-05
-- =============================================================================

-- Conectar como superuser (postgres)
\c postgres

-- Criar database principal (se não existir)
SELECT 'CREATE DATABASE sptrans_pipeline'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'sptrans_pipeline')\gexec

-- Conectar ao database
\c sptrans_pipeline

-- =============================================================================
-- EXTENSÕES
-- =============================================================================

-- PostGIS (para dados geoespaciais)
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

-- pg_stat_statements (para análise de performance)
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- uuid-ossp (para geração de UUIDs)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- btree_gin (para índices GIN em tipos numéricos)
CREATE EXTENSION IF NOT EXISTS btree_gin;

-- pg_trgm (para busca de texto similar)
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- =============================================================================
-- ROLES E PERMISSIONS
-- =============================================================================

-- Role para aplicação (read/write)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'sptrans_app') THEN
        CREATE ROLE sptrans_app WITH LOGIN PASSWORD 'sptrans_app_password';
    END IF;
END
$$;

-- Role para leitura apenas (dashboards)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'sptrans_readonly') THEN
        CREATE ROLE sptrans_readonly WITH LOGIN PASSWORD 'sptrans_readonly_password';
    END IF;
END
$$;

-- =============================================================================
-- CONFIGURAÇÕES DO DATABASE
-- =============================================================================

-- Timezone
ALTER DATABASE sptrans_pipeline SET timezone TO 'America/Sao_Paulo';

-- Configurações de performance
ALTER DATABASE sptrans_pipeline SET shared_buffers TO '256MB';
ALTER DATABASE sptrans_pipeline SET effective_cache_size TO '1GB';
ALTER DATABASE sptrans_pipeline SET maintenance_work_mem TO '64MB';
ALTER DATABASE sptrans_pipeline SET checkpoint_completion_target TO '0.9';
ALTER DATABASE sptrans_pipeline SET wal_buffers TO '16MB';
ALTER DATABASE sptrans_pipeline SET default_statistics_target TO '100';
ALTER DATABASE sptrans_pipeline SET random_page_cost TO '1.1';

-- Logging
ALTER DATABASE sptrans_pipeline SET log_statement TO 'mod';
ALTER DATABASE sptrans_pipeline SET log_duration TO on;
ALTER DATABASE sptrans_pipeline SET log_min_duration_statement TO '1000'; -- 1s

-- =============================================================================
-- VERIFICAÇÃO
-- =============================================================================

-- Listar extensões instaladas
SELECT 
    extname AS "Extension",
    extversion AS "Version"
FROM pg_extension
ORDER BY extname;

-- Listar roles criados
SELECT 
    rolname AS "Role",
    rolcanlogin AS "Can Login",
    rolcreatedb AS "Create DB"
FROM pg_roles
WHERE rolname LIKE 'sptrans%'
ORDER BY rolname;

-- =============================================================================
-- COMENTÁRIOS
-- =============================================================================

COMMENT ON DATABASE sptrans_pipeline IS 
'Database principal do pipeline de dados SPTrans - Sistema de transporte público de São Paulo';

-- =============================================================================
-- FIM DO SCRIPT
-- =============================================================================

\echo '✅ Database, extensões e roles criados com sucesso!'
\echo '📊 Estatísticas:'
\echo '   • Database: sptrans_pipeline'
\echo '   • Extensões: 6 instaladas'
\echo '   • Roles: 2 criados'
\echo ''
\echo '🔐 Credentials:'
\echo '   • App User: sptrans_app / sptrans_app_password'
\echo '   • Read-only User: sptrans_readonly / sptrans_readonly_password'
\echo ''
\echo '➡️  Próximo script: 01_serving_schema.sql'

#!/bin/bash

################################################################################
# Script: restore_data.sh
# Descrição: Restaura dados do backup do Data Lake (MinIO) e PostgreSQL
# Projeto: SPTrans Real-Time Data Pipeline
# Autor: Equipe LABDATA/FIA
################################################################################

set -e  # Exit on error

# ============================================================================
# CONFIGURAÇÕES
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Diretórios
BACKUP_DIR="${PROJECT_ROOT}/data/backups"
LOGS_DIR="${PROJECT_ROOT}/logs"

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Timestamp
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="${LOGS_DIR}/restore_${TIMESTAMP}.log"

# ============================================================================
# FUNÇÕES AUXILIARES
# ============================================================================

log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1" | tee -a "$LOG_FILE"
}

log_error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ERROR:${NC} $1" | tee -a "$LOG_FILE"
}

log_warning() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] WARNING:${NC} $1" | tee -a "$LOG_FILE"
}

log_info() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')] INFO:${NC} $1" | tee -a "$LOG_FILE"
}

check_requirements() {
    log "Verificando dependências..."
    
    local missing_deps=()
    
    command -v docker >/dev/null 2>&1 || missing_deps+=("docker")
    command -v docker-compose >/dev/null 2>&1 || missing_deps+=("docker-compose")
    command -v mc >/dev/null 2>&1 || missing_deps+=("mc (MinIO Client)")
    command -v psql >/dev/null 2>&1 || missing_deps+=("psql")
    
    if [ ${#missing_deps[@]} -ne 0 ]; then
        log_error "Dependências faltando: ${missing_deps[*]}"
        exit 1
    fi
    
    log "✓ Todas as dependências encontradas"
}

list_backups() {
    log_info "Backups disponíveis em ${BACKUP_DIR}:"
    echo ""
    
    if [ ! -d "$BACKUP_DIR" ] || [ -z "$(ls -A $BACKUP_DIR 2>/dev/null)" ]; then
        log_warning "Nenhum backup encontrado!"
        exit 1
    fi
    
    local count=1
    for backup in $(ls -t "$BACKUP_DIR"); do
        if [ -d "$BACKUP_DIR/$backup" ]; then
            echo "  [$count] $backup"
            ((count++))
        fi
    done
    echo ""
}

select_backup() {
    list_backups
    
    read -p "Digite o número ou nome do backup para restaurar: " selection
    
    if [[ "$selection" =~ ^[0-9]+$ ]]; then
        # Seleção por número
        SELECTED_BACKUP=$(ls -t "$BACKUP_DIR" | sed -n "${selection}p")
    else
        # Seleção por nome
        SELECTED_BACKUP="$selection"
    fi
    
    if [ ! -d "$BACKUP_DIR/$SELECTED_BACKUP" ]; then
        log_error "Backup não encontrado: $SELECTED_BACKUP"
        exit 1
    fi
    
    RESTORE_PATH="$BACKUP_DIR/$SELECTED_BACKUP"
    log "Backup selecionado: $SELECTED_BACKUP"
}

confirm_restore() {
    log_warning "ATENÇÃO: Esta operação irá SOBRESCREVER os dados atuais!"
    read -p "Deseja continuar? (yes/no): " confirm
    
    if [ "$confirm" != "yes" ]; then
        log "Restore cancelado pelo usuário"
        exit 0
    fi
}

# ============================================================================
# FUNÇÕES DE RESTORE
# ============================================================================

restore_minio() {
    log "Restaurando dados do MinIO..."
    
    local minio_backup="${RESTORE_PATH}/minio"
    
    if [ ! -d "$minio_backup" ]; then
        log_warning "Backup do MinIO não encontrado, pulando..."
        return
    fi
    
    # Configurar MinIO Client
    mc alias set local http://localhost:9000 minioadmin minioadmin
    
    # Restaurar buckets
    log_info "Restaurando buckets do MinIO..."
    
    for layer in bronze silver gold; do
        if [ -d "$minio_backup/sptrans-datalake/$layer" ]; then
            log_info "Restaurando camada: $layer"
            mc cp --recursive "$minio_backup/sptrans-datalake/$layer/" \
                "local/sptrans-datalake/$layer/"
        fi
    done
    
    log "✓ Restore do MinIO concluído"
}

restore_postgres() {
    log "Restaurando banco de dados PostgreSQL..."
    
    local postgres_backup="${RESTORE_PATH}/postgres"
    
    if [ ! -d "$postgres_backup" ]; then
        log_warning "Backup do PostgreSQL não encontrado, pulando..."
        return
    fi
    
    # Variáveis de conexão
    local POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
    local POSTGRES_PORT="${POSTGRES_PORT:-5432}"
    local POSTGRES_DB="${POSTGRES_DB:-sptrans}"
    local POSTGRES_USER="${POSTGRES_USER:-postgres}"
    
    # Restaurar cada schema
    for schema_backup in "$postgres_backup"/*.sql; do
        if [ -f "$schema_backup" ]; then
            local schema_name=$(basename "$schema_backup" .sql)
            log_info "Restaurando schema: $schema_name"
            
            PGPASSWORD="${POSTGRES_PASSWORD}" psql \
                -h "$POSTGRES_HOST" \
                -p "$POSTGRES_PORT" \
                -U "$POSTGRES_USER" \
                -d "$POSTGRES_DB" \
                -f "$schema_backup" 2>&1 | tee -a "$LOG_FILE"
        fi
    done
    
    log "✓ Restore do PostgreSQL concluído"
}

restore_metadata() {
    log "Restaurando metadados..."
    
    local metadata_backup="${RESTORE_PATH}/metadata"
    
    if [ ! -d "$metadata_backup" ]; then
        log_warning "Backup de metadados não encontrado, pulando..."
        return
    fi
    
    # Restaurar arquivos de configuração se existirem
    if [ -f "$metadata_backup/config.json" ]; then
        cp "$metadata_backup/config.json" "${PROJECT_ROOT}/config/"
        log_info "✓ Configurações restauradas"
    fi
    
    # Restaurar histórico de processamento
    if [ -f "$metadata_backup/processing_history.json" ]; then
        cp "$metadata_backup/processing_history.json" "${PROJECT_ROOT}/data/"
        log_info "✓ Histórico de processamento restaurado"
    fi
    
    log "✓ Restore de metadados concluído"
}

verify_restore() {
    log "Verificando integridade do restore..."
    
    local checks_passed=0
    local checks_failed=0
    
    # Verificar MinIO
    log_info "Verificando MinIO..."
    if mc ls local/sptrans-datalake/bronze/ >/dev/null 2>&1; then
        log "  ✓ Camada Bronze acessível"
        ((checks_passed++))
    else
        log_error "  ✗ Camada Bronze inacessível"
        ((checks_failed++))
    fi
    
    # Verificar PostgreSQL
    log_info "Verificando PostgreSQL..."
    if PGPASSWORD="${POSTGRES_PASSWORD}" psql -h localhost -U postgres -d sptrans \
        -c "SELECT COUNT(*) FROM gold.kpis_hourly LIMIT 1;" >/dev/null 2>&1; then
        log "  ✓ PostgreSQL acessível"
        ((checks_passed++))
    else
        log_error "  ✗ PostgreSQL inacessível"
        ((checks_failed++))
    fi
    
    echo ""
    log "Verificação concluída: $checks_passed OK, $checks_failed FAILED"
    
    if [ $checks_failed -gt 0 ]; then
        log_warning "Restore concluído com erros. Verifique o log: $LOG_FILE"
        exit 1
    fi
}

generate_restore_report() {
    log "Gerando relatório de restore..."
    
    local report_file="${RESTORE_PATH}/restore_report_${TIMESTAMP}.txt"
    
    cat > "$report_file" << EOF
================================================================================
RELATÓRIO DE RESTORE - SPTrans Data Pipeline
================================================================================

Data/Hora: $(date)
Backup Restaurado: $SELECTED_BACKUP
Restore Path: $RESTORE_PATH

================================================================================
COMPONENTES RESTAURADOS
================================================================================

1. MinIO (Data Lake):
   - Bronze Layer: $(mc du local/sptrans-datalake/bronze/ 2>/dev/null | awk '{print $1}' || echo "N/A")
   - Silver Layer: $(mc du local/sptrans-datalake/silver/ 2>/dev/null | awk '{print $1}' || echo "N/A")
   - Gold Layer: $(mc du local/sptrans-datalake/gold/ 2>/dev/null | awk '{print $1}' || echo "N/A")

2. PostgreSQL:
   - Database: sptrans
   - Schemas restaurados: bronze, silver, gold, serving

3. Metadados:
   - Configurações: Restauradas
   - Histórico: Restaurado

================================================================================
PRÓXIMOS PASSOS
================================================================================

1. Verificar logs de aplicação em: ${LOGS_DIR}
2. Validar dados restaurados executando queries de teste
3. Reiniciar serviços Airflow para sincronizar estado
4. Executar DAG de qualidade de dados

================================================================================
LOG COMPLETO
================================================================================

Consulte: $LOG_FILE

================================================================================
EOF

    log "✓ Relatório gerado: $report_file"
    cat "$report_file"
}

# ============================================================================
# FUNÇÃO PRINCIPAL
# ============================================================================

main() {
    echo ""
    echo "╔════════════════════════════════════════════════════════════════╗"
    echo "║        SPTrans Data Pipeline - Script de Restore               ║"
    echo "╚════════════════════════════════════════════════════════════════╝"
    echo ""
    
    # Criar diretório de logs se não existir
    mkdir -p "$LOGS_DIR"
    
    log "Iniciando processo de restore..."
    
    # Verificar dependências
    check_requirements
    
    # Selecionar backup
    select_backup
    
    # Confirmar operação
    confirm_restore
    
    # Executar restore
    log "Iniciando restore dos componentes..."
    
    restore_minio
    restore_postgres
    restore_metadata
    
    # Verificar integridade
    verify_restore
    
    # Gerar relatório
    generate_restore_report
    
    log "✓ Restore concluído com sucesso!"
    log_info "Para verificar detalhes, consulte: $LOG_FILE"
}

# ============================================================================
# EXECUÇÃO
# ============================================================================

# Carregar variáveis de ambiente se existirem
if [ -f "${PROJECT_ROOT}/.env" ]; then
    source "${PROJECT_ROOT}/.env"
fi

# Executar função principal
main "$@"

exit 0

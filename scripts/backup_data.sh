#!/bin/bash

################################################################################
# SPTRANS PIPELINE - BACKUP SCRIPT
################################################################################
# Script para backup de dados críticos do pipeline
#
# Faz backup de:
# - PostgreSQL (schemas e dados)
# - MinIO (Data Lake)
# - Configurações
#
# Uso: ./scripts/backup_data.sh [--full|--incremental]
################################################################################

set -e

# Cores
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Banner
echo "
╔══════════════════════════════════════════════════════════════╗
║                                                              ║
║         SPTRANS PIPELINE - BACKUP SCRIPT                     ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝
"

################################################################################
# CONFIGURAÇÕES
################################################################################

# Diretório de backup
BACKUP_DIR="data/backups"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_NAME="sptrans_backup_${TIMESTAMP}"
BACKUP_PATH="${BACKUP_DIR}/${BACKUP_NAME}"

# Tipo de backup (padrão: full)
BACKUP_TYPE="full"

# Parse arguments
if [[ "$1" == "--incremental" ]]; then
    BACKUP_TYPE="incremental"
fi

print_info "Tipo de backup: ${BACKUP_TYPE}"

################################################################################
# CRIAR DIRETÓRIO DE BACKUP
################################################################################

print_info "Criando diretório de backup..."
mkdir -p "${BACKUP_PATH}"
mkdir -p "${BACKUP_PATH}/postgresql"
mkdir -p "${BACKUP_PATH}/minio"
mkdir -p "${BACKUP_PATH}/configs"

print_success "Diretório criado: ${BACKUP_PATH}"

################################################################################
# BACKUP POSTGRESQL
################################################################################

print_info "Iniciando backup do PostgreSQL..."

# Verificar se PostgreSQL está rodando
if ! docker ps | grep -q postgres; then
    print_error "PostgreSQL não está rodando!"
    exit 1
fi

# Backup do database Airflow
print_info "Backup do database Airflow..."
docker exec postgres pg_dump -U airflow airflow > "${BACKUP_PATH}/postgresql/airflow_db.sql"
AIRFLOW_SIZE=$(du -h "${BACKUP_PATH}/postgresql/airflow_db.sql" | cut -f1)
print_success "Airflow DB: ${AIRFLOW_SIZE}"

# Backup do schema serving
print_info "Backup do schema serving..."
docker exec postgres pg_dump -U airflow -n serving airflow > "${BACKUP_PATH}/postgresql/serving_schema.sql"
SERVING_SIZE=$(du -h "${BACKUP_PATH}/postgresql/serving_schema.sql" | cut -f1)
print_success "Serving schema: ${SERVING_SIZE}"

# Backup de roles e permissões
print_info "Backup de roles..."
docker exec postgres pg_dumpall -U airflow --roles-only > "${BACKUP_PATH}/postgresql/roles.sql"

print_success "Backup PostgreSQL concluído"

################################################################################
# BACKUP MINIO (DATA LAKE)
################################################################################

print_info "Iniciando backup do MinIO (Data Lake)..."

# Verificar se MinIO está rodando
if ! docker ps | grep -q minio; then
    print_warning "MinIO não está rodando, pulando backup do Data Lake"
else
    # Instalar mc (MinIO Client) se necessário
    if ! docker exec minio which mc > /dev/null 2>&1; then
        print_warning "MinIO Client não disponível, pulando backup"
    else
        # Backup do bucket sptrans-datalake
        print_info "Backup do bucket sptrans-datalake..."
        
        # Configurar alias
        docker exec minio mc alias set local http://localhost:9000 admin miniopassword123 2>/dev/null || true
        
        if [ "$BACKUP_TYPE" == "full" ]; then
            # Backup completo
            print_info "Executando backup COMPLETO do Data Lake..."
            docker exec minio mc mirror local/sptrans-datalake /tmp/backup-minio 2>/dev/null || true
            
            # Copiar para host
            docker cp minio:/tmp/backup-minio "${BACKUP_PATH}/minio/" 2>/dev/null || true
            
            MINIO_SIZE=$(du -sh "${BACKUP_PATH}/minio/" 2>/dev/null | cut -f1 || echo "0")
            print_success "Data Lake backup: ${MINIO_SIZE}"
        else
            # Backup incremental (apenas metadados e últimas 24h)
            print_info "Executando backup INCREMENTAL (últimas 24h)..."
            
            YESTERDAY=$(date -d "yesterday" +%Y-%m-%d)
            
            # Backup apenas das partições recentes
            docker exec minio mc mirror \
                --newer-than "${YESTERDAY}" \
                local/sptrans-datalake \
                /tmp/backup-minio 2>/dev/null || true
            
            docker cp minio:/tmp/backup-minio "${BACKUP_PATH}/minio/" 2>/dev/null || true
            
            MINIO_SIZE=$(du -sh "${BACKUP_PATH}/minio/" 2>/dev/null | cut -f1 || echo "0")
            print_success "Data Lake incremental: ${MINIO_SIZE}"
        fi
    fi
fi

################################################################################
# BACKUP CONFIGURAÇÕES
################################################################################

print_info "Fazendo backup de configurações..."

# Copiar arquivos de configuração importantes
cp -r config/* "${BACKUP_PATH}/configs/" 2>/dev/null || true
cp .env "${BACKUP_PATH}/configs/.env.backup" 2>/dev/null || true
cp docker-compose.yml "${BACKUP_PATH}/configs/" 2>/dev/null || true
cp requirements.txt "${BACKUP_PATH}/configs/" 2>/dev/null || true

print_success "Configurações salvas"

################################################################################
# BACKUP LOGS (ÚLTIMAS 24h)
################################################################################

print_info "Fazendo backup de logs (últimas 24h)..."

# Encontrar logs das últimas 24h
find logs/ -type f -mtime -1 -exec cp --parents {} "${BACKUP_PATH}/" \; 2>/dev/null || true

LOGS_SIZE=$(du -sh "${BACKUP_PATH}/logs/" 2>/dev/null | cut -f1 || echo "0")
print_success "Logs salvos: ${LOGS_SIZE}"

################################################################################
# CRIAR MANIFESTO
################################################################################

print_info "Criando manifesto do backup..."

cat > "${BACKUP_PATH}/MANIFEST.txt" << EOF
SPTRANS PIPELINE - BACKUP MANIFEST
====================================

Backup ID: ${BACKUP_NAME}
Data/Hora: $(date)
Tipo: ${BACKUP_TYPE}

CONTEÚDO:
---------
PostgreSQL:
  - Airflow DB: ${AIRFLOW_SIZE}
  - Serving Schema: ${SERVING_SIZE}
  - Roles: incluído

MinIO (Data Lake):
  - Bucket: sptrans-datalake
  - Tamanho: ${MINIO_SIZE}

Configurações:
  - config/
  - .env
  - docker-compose.yml
  - requirements.txt

Logs:
  - Últimas 24 horas
  - Tamanho: ${LOGS_SIZE}

RESTAURAÇÃO:
------------
Para restaurar este backup:
  ./scripts/restore_data.sh ${BACKUP_NAME}

NOTAS:
------
- Backup criado por: $(whoami)
- Host: $(hostname)
- Sistema: $(uname -a)
EOF

print_success "Manifesto criado"

################################################################################
# COMPRIMIR BACKUP
################################################################################

print_info "Comprimindo backup..."

cd "${BACKUP_DIR}"
tar -czf "${BACKUP_NAME}.tar.gz" "${BACKUP_NAME}/" 2>/dev/null

if [ -f "${BACKUP_NAME}.tar.gz" ]; then
    COMPRESSED_SIZE=$(du -h "${BACKUP_NAME}.tar.gz" | cut -f1)
    print_success "Backup comprimido: ${COMPRESSED_SIZE}"
    
    # Remover diretório não comprimido
    rm -rf "${BACKUP_NAME}"
    
    # Calcular checksum
    CHECKSUM=$(md5sum "${BACKUP_NAME}.tar.gz" | cut -d' ' -f1)
    echo "${CHECKSUM}" > "${BACKUP_NAME}.tar.gz.md5"
    
    print_success "Checksum MD5: ${CHECKSUM}"
else
    print_error "Falha ao comprimir backup"
    cd - > /dev/null
    exit 1
fi

cd - > /dev/null

################################################################################
# LIMPEZA DE BACKUPS ANTIGOS
################################################################################

print_info "Verificando backups antigos..."

# Manter apenas últimos 7 backups
BACKUP_COUNT=$(find "${BACKUP_DIR}" -name "sptrans_backup_*.tar.gz" | wc -l)

if [ ${BACKUP_COUNT} -gt 7 ]; then
    print_warning "Existem ${BACKUP_COUNT} backups. Removendo os mais antigos..."
    
    # Listar backups do mais antigo ao mais novo, remover excedentes
    ls -t "${BACKUP_DIR}"/sptrans_backup_*.tar.gz | tail -n +8 | while read old_backup; do
        print_info "Removendo backup antigo: $(basename ${old_backup})"
        rm -f "${old_backup}"
        rm -f "${old_backup}.md5"
    done
    
    print_success "Limpeza concluída"
fi

################################################################################
# ESTATÍSTICAS
################################################################################

echo ""
print_info "═══════════════════════════════════════════════════════════"
print_success "Estatísticas do Backup:"
print_info "═══════════════════════════════════════════════════════════"
echo ""
echo "  📦 Arquivo: ${BACKUP_NAME}.tar.gz"
echo "  📊 Tamanho: ${COMPRESSED_SIZE}"
echo "  🔐 MD5: ${CHECKSUM}"
echo "  📅 Data: $(date)"
echo "  ⏱️  Tipo: ${BACKUP_TYPE}"
echo ""
print_info "═══════════════════════════════════════════════════════════"

################################################################################
# FINALIZAÇÃO
################################################################################

echo ""
print_success "╔══════════════════════════════════════════════════════════════╗"
print_success "║                                                              ║"
print_success "║            ✅ BACKUP CONCLUÍDO COM SUCESSO!                  ║"
print_success "║                                                              ║"
print_success "╚══════════════════════════════════════════════════════════════╝"
echo ""

print_info "Backup salvo em:"
echo "  ${BACKUP_DIR}/${BACKUP_NAME}.tar.gz"
echo ""

print_info "Para restaurar este backup:"
echo "  ./scripts/restore_data.sh ${BACKUP_NAME}"
echo ""

print_success "Backup finalizado! 💾"
echo ""

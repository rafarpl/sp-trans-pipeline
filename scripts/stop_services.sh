#!/bin/bash

################################################################################
# SPTRANS REAL-TIME PIPELINE - STOP SERVICES
################################################################################
# Script para parar todos os serviços do pipeline
#
# Uso: ./scripts/stop_services.sh [options]
# Options:
#   --remove-volumes : Remove volumes (CUIDADO: apaga todos os dados!)
#   --force         : Para containers imediatamente sem graceful shutdown
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
║         SPTRANS PIPELINE - STOPPING SERVICES                 ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝
"

################################################################################
# PARSE ARGUMENTS
################################################################################

REMOVE_VOLUMES=false
FORCE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --remove-volumes)
            REMOVE_VOLUMES=true
            shift
            ;;
        --force)
            FORCE=true
            shift
            ;;
        *)
            print_error "Opção desconhecida: $1"
            echo "Uso: $0 [--remove-volumes] [--force]"
            exit 1
            ;;
    esac
done

################################################################################
# CONFIRMAÇÃO PARA REMOVER VOLUMES
################################################################################

if [ "$REMOVE_VOLUMES" = true ]; then
    print_warning "╔════════════════════════════════════════════════════════╗"
    print_warning "║                                                        ║"
    print_warning "║                    ⚠️  ATENÇÃO  ⚠️                     ║"
    print_warning "║                                                        ║"
    print_warning "║  Você está prestes a REMOVER TODOS OS VOLUMES!        ║"
    print_warning "║  Isso irá APAGAR PERMANENTEMENTE:                     ║"
    print_warning "║                                                        ║"
    print_warning "║  • Todos os dados do PostgreSQL                       ║"
    print_warning "║  • Todos os dados do MinIO (Data Lake)                ║"
    print_warning "║  • Todos os dados do Redis                            ║"
    print_warning "║  • Histórico de execuções do Airflow                  ║"
    print_warning "║                                                        ║"
    print_warning "╚════════════════════════════════════════════════════════╝"
    echo ""
    
    read -p "Tem certeza que deseja continuar? Digite 'yes' para confirmar: " confirmation
    
    if [ "$confirmation" != "yes" ]; then
        print_info "Operação cancelada"
        exit 0
    fi
    
    print_warning "Última chance! Digite 'DELETE' para confirmar: "
    read -p "" final_confirmation
    
    if [ "$final_confirmation" != "DELETE" ]; then
        print_info "Operação cancelada"
        exit 0
    fi
    
    print_warning "Prosseguindo com remoção de volumes..."
fi

################################################################################
# VERIFICAR DOCKER
################################################################################

if ! docker info > /dev/null 2>&1; then
    print_error "Docker não está rodando!"
    exit 1
fi

################################################################################
# BACKUP RÁPIDO (se volumes não forem removidos)
################################################################################

if [ "$REMOVE_VOLUMES" = false ]; then
    print_info "Verificando se há dados para backup..."
    
    # Verificar se containers estão rodando
    if docker-compose ps | grep -q "Up"; then
        print_info "Criando backup rápido do PostgreSQL..."
        
        timestamp=$(date +%Y%m%d_%H%M%S)
        backup_dir="data/backups/stop_backup_$timestamp"
        mkdir -p "$backup_dir"
        
        # Backup do PostgreSQL
        docker exec postgres pg_dump -U airflow airflow > "$backup_dir/airflow_db.sql" 2>/dev/null || \
            print_warning "Não foi possível fazer backup do PostgreSQL"
        
        if [ -f "$backup_dir/airflow_db.sql" ]; then
            print_success "Backup salvo em: $backup_dir"
        fi
    fi
fi

################################################################################
# PARAR SERVIÇOS
################################################################################

print_info "Parando serviços..."

if [ "$FORCE" = true ]; then
    print_warning "Parando containers forçadamente..."
    docker-compose kill
else
    print_info "Parando containers gracefully..."
    
    # Parar em ordem reversa
    print_info "Parando serviços de monitoramento..."
    docker-compose stop grafana prometheus 2>/dev/null || true
    
    print_info "Parando Airflow..."
    docker-compose stop airflow-triggerer airflow-scheduler airflow-webserver 2>/dev/null || true
    
    print_info "Parando Spark..."
    docker-compose stop spark-worker spark-master 2>/dev/null || true
    
    print_info "Parando infraestrutura base..."
    docker-compose stop minio redis postgres 2>/dev/null || true
fi

print_success "Containers parados"

################################################################################
# REMOVER CONTAINERS
################################################################################

print_info "Removendo containers..."

if [ "$REMOVE_VOLUMES" = true ]; then
    docker-compose down -v
    print_warning "Containers e volumes removidos"
else
    docker-compose down
    print_success "Containers removidos (volumes preservados)"
fi

################################################################################
# LIMPAR RECURSOS ÓRFÃOS
################################################################################

print_info "Limpando recursos órfãos..."

# Remover networks órfãs
docker network prune -f 2>/dev/null || true

# Remover containers parados
docker container prune -f 2>/dev/null || true

print_success "Limpeza concluída"

################################################################################
# VERIFICAR STATUS
################################################################################

print_info "Verificando containers restantes..."

remaining_containers=$(docker ps -a --filter "name=sptrans" --format "{{.Names}}" | wc -l)

if [ "$remaining_containers" -eq 0 ]; then
    print_success "Nenhum container do projeto em execução"
else
    print_warning "Ainda existem $remaining_containers containers:"
    docker ps -a --filter "name=sptrans"
fi

################################################################################
# ESTATÍSTICAS DE DISCO
################################################################################

if [ "$REMOVE_VOLUMES" = true ]; then
    print_info "Verificando espaço em disco liberado..."
    
    # Mostrar volumes Docker
    print_info "Volumes Docker restantes:"
    docker volume ls | grep sptrans || print_success "Nenhum volume do projeto"
    
    # Sugerir limpeza adicional
    echo ""
    print_info "Para liberar mais espaço, você pode executar:"
    echo "  • docker system prune -a     (remove imagens não usadas)"
    echo "  • docker volume prune        (remove volumes não usados)"
    echo ""
fi

################################################################################
# INFORMAÇÕES DE LOGS
################################################################################

if [ "$REMOVE_VOLUMES" = false ]; then
    print_info "Logs preservados em:"
    echo "  • logs/airflow/"
    echo "  • logs/spark/"
    echo "  • logs/application/"
    echo ""
    print_info "Para limpar logs: rm -rf logs/*"
fi

################################################################################
# FINALIZAÇÃO
################################################################################

echo ""
print_success "╔══════════════════════════════════════════════════════════════╗"
print_success "║                                                              ║"
print_success "║            ✅ SERVIÇOS PARADOS COM SUCESSO!                  ║"
print_success "║                                                              ║"
print_success "╚══════════════════════════════════════════════════════════════╝"
echo ""

if [ "$REMOVE_VOLUMES" = true ]; then
    print_warning "Volumes foram removidos. Todos os dados foram apagados."
    echo ""
    print_info "Para reiniciar do zero:"
    echo "  1. ./scripts/setup.sh"
    echo "  2. ./scripts/start_services.sh"
else
    print_success "Volumes foram preservados. Dados estão seguros."
    echo ""
    print_info "Para reiniciar os serviços:"
    echo "  ./scripts/start_services.sh"
fi

echo ""
print_info "Opções avançadas:"
echo ""
echo "  • Parar e remover tudo:        $0 --remove-volumes"
echo "  • Parar forçadamente:          $0 --force"
echo "  • Limpar sistema completo:     docker system prune -a --volumes"
echo ""

print_success "Pipeline finalizado! 👋"
echo ""

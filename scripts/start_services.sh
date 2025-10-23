#!/bin/bash

################################################################################
# SPTRANS REAL-TIME PIPELINE - START SERVICES
################################################################################
# Script para iniciar todos os serviços do pipeline
#
# Uso: ./scripts/start_services.sh [options]
# Options:
#   --build     : Rebuild images antes de iniciar
#   --no-wait   : Não aguardar health checks
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
║         SPTRANS PIPELINE - STARTING SERVICES                 ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝
"

################################################################################
# PARSE ARGUMENTS
################################################################################

BUILD=false
WAIT=true

while [[ $# -gt 0 ]]; do
    case $1 in
        --build)
            BUILD=true
            shift
            ;;
        --no-wait)
            WAIT=false
            shift
            ;;
        *)
            print_error "Opção desconhecida: $1"
            echo "Uso: $0 [--build] [--no-wait]"
            exit 1
            ;;
    esac
done

################################################################################
# VERIFICAR DOCKER
################################################################################

print_info "Verificando Docker..."

if ! docker info > /dev/null 2>&1; then
    print_error "Docker não está rodando!"
    print_info "Inicie o Docker e tente novamente"
    exit 1
fi

print_success "Docker está rodando"

################################################################################
# VERIFICAR ARQUIVO .env
################################################################################

print_info "Verificando arquivo .env..."

if [ ! -f ".env" ]; then
    print_error "Arquivo .env não encontrado!"
    print_info "Execute primeiro: ./scripts/setup.sh"
    exit 1
fi

print_success "Arquivo .env encontrado"

################################################################################
# BUILD (OPCIONAL)
################################################################################

if [ "$BUILD" = true ]; then
    print_info "Building Docker images..."
    docker-compose build --no-cache
    print_success "Build concluído"
fi

################################################################################
# PARAR CONTAINERS ANTIGOS
################################################################################

print_info "Parando containers antigos (se existirem)..."
docker-compose down 2>/dev/null || true
print_success "Containers antigos parados"

################################################################################
# INICIAR SERVIÇOS
################################################################################

print_info "Iniciando serviços..."

# Criar network se não existir
docker network create sptrans-network 2>/dev/null || true

# Iniciar serviços em ordem
print_info "Iniciando infraestrutura base..."
docker-compose up -d postgres redis minio

if [ "$WAIT" = true ]; then
    print_info "Aguardando infraestrutura base (30s)..."
    sleep 30
fi

print_info "Iniciando Spark..."
docker-compose up -d spark-master spark-worker

if [ "$WAIT" = true ]; then
    print_info "Aguardando Spark (15s)..."
    sleep 15
fi

print_info "Iniciando Airflow..."
docker-compose up -d airflow-webserver airflow-scheduler airflow-triggerer

if [ "$WAIT" = true ]; then
    print_info "Aguardando Airflow (20s)..."
    sleep 20
fi

print_info "Iniciando serviços de monitoramento..."
docker-compose up -d prometheus grafana

print_success "Todos os serviços iniciados!"

################################################################################
# HEALTH CHECKS
################################################################################

if [ "$WAIT" = true ]; then
    print_info "Executando health checks..."
    echo ""
    
    # PostgreSQL
    print_info "Verificando PostgreSQL..."
    for i in {1..10}; do
        if docker exec postgres pg_isready -U airflow > /dev/null 2>&1; then
            print_success "PostgreSQL: HEALTHY ✓"
            break
        fi
        if [ $i -eq 10 ]; then
            print_error "PostgreSQL: TIMEOUT ✗"
        else
            sleep 3
        fi
    done
    
    # Redis
    print_info "Verificando Redis..."
    for i in {1..5}; do
        if docker exec redis redis-cli ping > /dev/null 2>&1; then
            print_success "Redis: HEALTHY ✓"
            break
        fi
        if [ $i -eq 5 ]; then
            print_error "Redis: TIMEOUT ✗"
        else
            sleep 2
        fi
    done
    
    # MinIO
    print_info "Verificando MinIO..."
    for i in {1..10}; do
        if curl -sf http://localhost:9000/minio/health/live > /dev/null 2>&1; then
            print_success "MinIO: HEALTHY ✓"
            break
        fi
        if [ $i -eq 10 ]; then
            print_error "MinIO: TIMEOUT ✗"
        else
            sleep 3
        fi
    done
    
    # Spark Master
    print_info "Verificando Spark Master..."
    for i in {1..10}; do
        if curl -sf http://localhost:8081 > /dev/null 2>&1; then
            print_success "Spark Master: HEALTHY ✓"
            break
        fi
        if [ $i -eq 10 ]; then
            print_warning "Spark Master: Não respondendo"
        else
            sleep 3
        fi
    done
    
    # Airflow Webserver
    print_info "Verificando Airflow Webserver..."
    for i in {1..15}; do
        if curl -sf http://localhost:8080/health > /dev/null 2>&1; then
            print_success "Airflow Webserver: HEALTHY ✓"
            break
        fi
        if [ $i -eq 15 ]; then
            print_warning "Airflow Webserver: Não respondendo (pode precisar de mais tempo)"
        else
            sleep 4
        fi
    done
    
    # Prometheus
    print_info "Verificando Prometheus..."
    if curl -sf http://localhost:9090/-/healthy > /dev/null 2>&1; then
        print_success "Prometheus: HEALTHY ✓"
    else
        print_warning "Prometheus: Não respondendo"
    fi
    
    # Grafana
    print_info "Verificando Grafana..."
    if curl -sf http://localhost:3000/api/health > /dev/null 2>&1; then
        print_success "Grafana: HEALTHY ✓"
    else
        print_warning "Grafana: Não respondendo"
    fi
fi

################################################################################
# CRIAR BUCKET MINIO
################################################################################

if [ "$WAIT" = true ]; then
    print_info "Criando bucket no MinIO..."
    
    sleep 5
    
    # Instalar mc (MinIO Client) se não existir
    docker exec minio mc alias set local http://localhost:9000 admin miniopassword123 2>/dev/null || true
    
    # Criar bucket
    docker exec minio mc mb local/sptrans-datalake 2>/dev/null || print_info "Bucket já existe"
    
    # Criar estrutura de pastas
    docker exec minio mc mb local/sptrans-datalake/bronze 2>/dev/null || true
    docker exec minio mc mb local/sptrans-datalake/silver 2>/dev/null || true
    docker exec minio mc mb local/sptrans-datalake/gold 2>/dev/null || true
    
    print_success "Bucket MinIO configurado"
fi

################################################################################
# EXIBIR STATUS
################################################################################

echo ""
print_info "═══════════════════════════════════════════════════════════"
print_success "Status dos Containers:"
print_info "═══════════════════════════════════════════════════════════"
echo ""

docker-compose ps

################################################################################
# INFORMAÇÕES DE ACESSO
################################################################################

echo ""
print_info "═══════════════════════════════════════════════════════════"
print_success "Serviços Disponíveis:"
print_info "═══════════════════════════════════════════════════════════"
echo ""
echo "  🌐 Airflow Webserver"
echo "     URL:  http://localhost:8080"
echo "     User: admin"
echo "     Pass: admin"
echo ""
echo "  💾 MinIO Console"
echo "     URL:  http://localhost:9001"
echo "     User: admin"
echo "     Pass: (conforme .env - default: miniopassword123)"
echo ""
echo "  📊 Grafana"
echo "     URL:  http://localhost:3000"
echo "     User: admin"
echo "     Pass: admin"
echo ""
echo "  🔥 Spark Master UI"
echo "     URL:  http://localhost:8081"
echo ""
echo "  📈 Prometheus"
echo "     URL:  http://localhost:9090"
echo ""
echo "  🗄️  PostgreSQL"
echo "     Host: localhost:5432"
echo "     User: airflow"
echo "     Pass: (conforme .env)"
echo "     DB:   airflow"
echo ""
echo "  🔴 Redis"
echo "     Host: localhost:6379"
echo ""
print_info "═══════════════════════════════════════════════════════════"

################################################################################
# COMANDOS ÚTEIS
################################################################################

echo ""
print_info "Comandos Úteis:"
echo ""
echo "  • Ver logs de todos serviços:    docker-compose logs -f"
echo "  • Ver logs de um serviço:        docker-compose logs -f <service>"
echo "  • Parar todos serviços:          ./scripts/stop_services.sh"
echo "  • Reiniciar um serviço:          docker-compose restart <service>"
echo "  • Ver status:                    docker-compose ps"
echo "  • Acessar container:             docker exec -it <container> bash"
echo ""

################################################################################
# VERIFICAR CONFIGURAÇÃO AIRFLOW
################################################################################

if [ "$WAIT" = true ]; then
    print_info "Verificando configuração do Airflow..."
    
    # Aguardar mais um pouco para Airflow estar completamente pronto
    sleep 10
    
    # Verificar se DAGs foram carregadas
    print_info "Verificando DAGs..."
    DAG_COUNT=$(docker exec airflow-webserver airflow dags list 2>/dev/null | wc -l)
    
    if [ "$DAG_COUNT" -gt 0 ]; then
        print_success "DAGs encontradas: $DAG_COUNT"
    else
        print_warning "Nenhuma DAG encontrada (normal na primeira inicialização)"
    fi
fi

################################################################################
# FINALIZAÇÃO
################################################################################

echo ""
print_success "╔══════════════════════════════════════════════════════════════╗"
print_success "║                                                              ║"
print_success "║            ✅ SERVIÇOS INICIADOS COM SUCESSO!                ║"
print_success "║                                                              ║"
print_success "╚══════════════════════════════════════════════════════════════╝"
echo ""

print_info "Próximos passos:"
echo ""
echo "  1. 🌐 Acesse o Airflow: http://localhost:8080"
echo "  2. 🔑 Configure o token da API SPTrans no .env"
echo "  3. 🚀 Ative as DAGs no Airflow"
echo "  4. 📊 Monitore em tempo real no Grafana"
echo ""

print_success "Pipeline pronto para uso! 🎉"
echo ""

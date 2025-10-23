#!/bin/bash

################################################################################
# SPTRANS REAL-TIME PIPELINE - SETUP SCRIPT
################################################################################
# Script de inicialização do projeto
# Cria diretórios, configura permissões e inicializa serviços
#
# Uso: ./scripts/setup.sh
################################################################################

set -e  # Exit on error

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Função para printar com cor
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
║         SPTRANS REAL-TIME PIPELINE - SETUP                   ║
║         Projeto de Engenharia de Dados                       ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝
"

################################################################################
# 1. VERIFICAR DEPENDÊNCIAS
################################################################################

print_info "Verificando dependências..."

# Verificar Docker
if ! command -v docker &> /dev/null; then
    print_error "Docker não está instalado!"
    print_info "Instale Docker: https://docs.docker.com/get-docker/"
    exit 1
fi
print_success "Docker encontrado: $(docker --version)"

# Verificar Docker Compose
if ! command -v docker-compose &> /dev/null; then
    print_error "Docker Compose não está instalado!"
    print_info "Instale Docker Compose: https://docs.docker.com/compose/install/"
    exit 1
fi
print_success "Docker Compose encontrado: $(docker-compose --version)"

# Verificar Python
if ! command -v python3 &> /dev/null; then
    print_warning "Python3 não encontrado. Recomendado para desenvolvimento local."
else
    print_success "Python encontrado: $(python3 --version)"
fi

# Verificar Git
if ! command -v git &> /dev/null; then
    print_warning "Git não encontrado. Necessário para versionamento."
else
    print_success "Git encontrado: $(git --version)"
fi

################################################################################
# 2. CRIAR ESTRUTURA DE DIRETÓRIOS
################################################################################

print_info "Criando estrutura de diretórios..."

# Diretórios principais
directories=(
    "logs/airflow"
    "logs/spark"
    "logs/application"
    "data/gtfs"
    "data/samples"
    "data/backups"
    "data/minio"
    "data/postgres"
    "data/redis"
    "config/airflow"
    "config/spark"
    "config/prometheus"
    "config/grafana/dashboards"
    "infra/docker"
    "infra/kubernetes"
    "infra/terraform"
    "docs/diagrams"
    "presentations"
    "tests/fixtures"
    "notebooks"
)

for dir in "${directories[@]}"; do
    if [ ! -d "$dir" ]; then
        mkdir -p "$dir"
        print_success "Criado: $dir"
    else
        print_info "Já existe: $dir"
    fi
done

# Criar .gitkeep em diretórios vazios
touch data/backups/.gitkeep
touch logs/airflow/.gitkeep
touch logs/spark/.gitkeep
touch logs/application/.gitkeep

################################################################################
# 3. CONFIGURAR PERMISSÕES
################################################################################

print_info "Configurando permissões..."

# Permissões para logs (read/write para todos)
chmod -R 777 logs/ 2>/dev/null || print_warning "Não foi possível alterar permissões de logs/"

# Permissões para data
chmod -R 777 data/ 2>/dev/null || print_warning "Não foi possível alterar permissões de data/"

# Tornar scripts executáveis
chmod +x scripts/*.sh 2>/dev/null || print_warning "Não foi possível tornar scripts executáveis"

print_success "Permissões configuradas"

################################################################################
# 4. CRIAR ARQUIVO .env SE NÃO EXISTIR
################################################################################

print_info "Verificando arquivo .env..."

if [ ! -f ".env" ]; then
    if [ -f ".env.example" ]; then
        cp .env.example .env
        print_success "Arquivo .env criado a partir do .env.example"
        print_warning "IMPORTANTE: Edite o arquivo .env e configure suas variáveis!"
        print_info "Especialmente: SPTRANS_API_TOKEN, POSTGRES_PASSWORD, MINIO_ROOT_PASSWORD"
    else
        print_error ".env.example não encontrado!"
        print_info "Criando .env básico..."
        
        cat > .env << 'EOF'
# SPTrans API
SPTRANS_API_TOKEN=seu_token_aqui
SPTRANS_API_URL=http://api.olhovivo.sptrans.com.br/v2.1

# PostgreSQL
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow_pass_change_me
POSTGRES_DB=airflow
POSTGRES_HOST=postgres
POSTGRES_PORT=5432

# MinIO (S3-Compatible)
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=minio_pass_change_me
MINIO_ENDPOINT=minio:9000
MINIO_BUCKET=sptrans-datalake

# Redis
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_PASSWORD=redis_pass_change_me

# Airflow
AIRFLOW_UID=50000
AIRFLOW_GID=0
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow_pass_change_me@postgres/airflow
AIRFLOW__CORE__FERNET_KEY=your_fernet_key_here
AIRFLOW__CORE__LOAD_EXAMPLES=False

# Spark
SPARK_MASTER_HOST=spark-master
SPARK_MASTER_PORT=7077
SPARK_MASTER_WEBUI_PORT=8080

# Environment
ENVIRONMENT=development
LOG_LEVEL=INFO
TZ=America/Sao_Paulo
EOF
        
        print_success "Arquivo .env básico criado"
        print_warning "IMPORTANTE: Edite o .env e configure as senhas e tokens!"
    fi
else
    print_success "Arquivo .env já existe"
fi

################################################################################
# 5. VERIFICAR PORTAS DISPONÍVEIS
################################################################################

print_info "Verificando portas disponíveis..."

ports_to_check=(
    "5432:PostgreSQL"
    "9000:MinIO API"
    "9001:MinIO Console"
    "6379:Redis"
    "8080:Airflow/Spark"
    "7077:Spark Master"
    "9090:Prometheus"
    "3000:Grafana"
)

for port_info in "${ports_to_check[@]}"; do
    IFS=':' read -r port service <<< "$port_info"
    
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1 ; then
        print_warning "Porta $port ($service) já está em uso!"
    else
        print_success "Porta $port ($service) disponível"
    fi
done

################################################################################
# 6. INSTALAR DEPENDÊNCIAS PYTHON (OPCIONAL)
################################################################################

print_info "Deseja instalar dependências Python localmente? (y/n)"
read -r install_python

if [[ "$install_python" == "y" || "$install_python" == "Y" ]]; then
    if command -v python3 &> /dev/null; then
        print_info "Instalando dependências Python..."
        
        # Criar virtual environment se não existir
        if [ ! -d "venv" ]; then
            python3 -m venv venv
            print_success "Virtual environment criado"
        fi
        
        # Ativar venv e instalar
        source venv/bin/activate
        pip install --upgrade pip
        pip install -r requirements.txt
        
        print_success "Dependências Python instaladas em venv/"
        print_info "Para ativar: source venv/bin/activate"
    else
        print_error "Python3 não encontrado. Pulando instalação de dependências."
    fi
else
    print_info "Pulando instalação de dependências Python"
fi

################################################################################
# 7. INICIALIZAR DOCKER CONTAINERS
################################################################################

print_info "Deseja inicializar os containers Docker agora? (y/n)"
read -r start_containers

if [[ "$start_containers" == "y" || "$start_containers" == "Y" ]]; then
    print_info "Iniciando containers Docker..."
    
    # Build das imagens
    print_info "Building Docker images..."
    docker-compose build
    
    # Iniciar serviços
    print_info "Starting services..."
    docker-compose up -d
    
    # Aguardar inicialização
    print_info "Aguardando inicialização dos serviços (30s)..."
    sleep 30
    
    # Verificar status
    print_info "Verificando status dos containers..."
    docker-compose ps
    
    print_success "Containers inicializados!"
    
    # Instruções de acesso
    echo ""
    print_info "═══════════════════════════════════════════════"
    print_success "Serviços disponíveis:"
    echo ""
    echo "  🌐 Airflow Webserver:  http://localhost:8080"
    echo "     User: admin / Pass: admin"
    echo ""
    echo "  💾 MinIO Console:      http://localhost:9001"
    echo "     User: admin / Pass: (conforme .env)"
    echo ""
    echo "  📊 Grafana:            http://localhost:3000"
    echo "     User: admin / Pass: admin"
    echo ""
    echo "  🔥 Spark Master UI:    http://localhost:8081"
    echo ""
    print_info "═══════════════════════════════════════════════"
else
    print_info "Containers não foram iniciados"
    print_info "Para iniciar manualmente: docker-compose up -d"
fi

################################################################################
# 8. CRIAR DATABASE E SCHEMA POSTGRESQL
################################################################################

if [[ "$start_containers" == "y" || "$start_containers" == "Y" ]]; then
    print_info "Deseja criar databases e schemas PostgreSQL? (y/n)"
    read -r create_db
    
    if [[ "$create_db" == "y" || "$create_db" == "Y" ]]; then
        print_info "Aguardando PostgreSQL estar pronto..."
        sleep 10
        
        print_info "Executando scripts SQL..."
        
        # Executar scripts SQL
        docker exec -i postgres psql -U airflow -d airflow < sql/00_create_databases.sql 2>/dev/null || print_warning "Erro ao executar 00_create_databases.sql"
        docker exec -i postgres psql -U airflow -d airflow < sql/01_serving_schema.sql 2>/dev/null || print_warning "Erro ao executar 01_serving_schema.sql"
        docker exec -i postgres psql -U airflow -d airflow < sql/02_serving_tables.sql 2>/dev/null || print_warning "Erro ao executar 02_serving_tables.sql"
        
        print_success "Databases e schemas criados!"
    fi
fi

################################################################################
# 9. VERIFICAR HEALTH DOS SERVIÇOS
################################################################################

if [[ "$start_containers" == "y" || "$start_containers" == "Y" ]]; then
    print_info "Verificando saúde dos serviços..."
    
    # Verificar PostgreSQL
    if docker exec postgres pg_isready -U airflow >/dev/null 2>&1; then
        print_success "PostgreSQL: HEALTHY ✓"
    else
        print_error "PostgreSQL: UNHEALTHY ✗"
    fi
    
    # Verificar MinIO
    if curl -s http://localhost:9000/minio/health/live >/dev/null 2>&1; then
        print_success "MinIO: HEALTHY ✓"
    else
        print_error "MinIO: UNHEALTHY ✗"
    fi
    
    # Verificar Redis
    if docker exec redis redis-cli ping >/dev/null 2>&1; then
        print_success "Redis: HEALTHY ✓"
    else
        print_error "Redis: UNHEALTHY ✗"
    fi
fi

################################################################################
# 10. FINALIZAÇÃO
################################################################################

echo ""
print_success "╔══════════════════════════════════════════════════════════════╗"
print_success "║                                                              ║"
print_success "║              ✅ SETUP CONCLUÍDO COM SUCESSO!                 ║"
print_success "║                                                              ║"
print_success "╚══════════════════════════════════════════════════════════════╝"
echo ""

print_info "Próximos passos:"
echo ""
echo "  1. ✏️  Editar arquivo .env com suas credenciais"
echo "  2. 🔑 Obter token da API SPTrans em:"
echo "      https://www.sptrans.com.br/desenvolvedores/"
echo "  3. 📚 Ler documentação em docs/"
echo "  4. 🧪 Executar testes: make test"
echo "  5. 🚀 Iniciar DAGs no Airflow"
echo ""

print_info "Comandos úteis:"
echo ""
echo "  • Ver logs:           docker-compose logs -f"
echo "  • Parar serviços:     docker-compose stop"
echo "  • Reiniciar:          docker-compose restart"
echo "  • Limpar tudo:        docker-compose down -v"
echo ""

print_success "Bom trabalho! 🎉"
echo ""

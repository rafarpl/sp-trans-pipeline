#!/bin/bash
# ============================================================================
# SETUP COMPLETO - SPTRANS PIPELINE
# ============================================================================

set -e
echo "🚀 Iniciando setup da VM..."
echo ""

# ============================================================================
# 1. ATUALIZAR SISTEMA
# ============================================================================
echo "📦 [1/8] Atualizando sistema..."
sudo apt-get update -qq

# ============================================================================
# 2. INSTALAR JAVA 11 (necessário para Spark)
# ============================================================================
echo "☕ [2/8] Instalando Java 11..."
sudo apt-get install -y -qq openjdk-11-jdk > /dev/null

# Configurar JAVA_HOME
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> ~/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc

echo "   ✓ Java instalado: $(java -version 2>&1 | head -n 1)"

# ============================================================================
# 3. CRIAR AMBIENTE VIRTUAL PYTHON
# ============================================================================
echo "🐍 [3/8] Criando ambiente virtual Python..."
python3 -m venv venv
source venv/bin/activate

echo "   ✓ Python: $(python3 --version)"

# ============================================================================
# 4. ATUALIZAR PIP
# ============================================================================
echo "📦 [4/8] Atualizando pip..."
pip install --quiet --upgrade pip setuptools wheel

# ============================================================================
# 5. INSTALAR DEPENDÊNCIAS PYTHON
# ============================================================================
echo "📦 [5/8] Instalando dependências Python (pode demorar 3-5 min)..."

# Core
pip install --quiet python-dotenv pydantic pydantic-settings

# PySpark e Delta Lake
pip install --quiet pyspark==3.5.0 delta-spark==3.0.0

# Data Processing
pip install --quiet pandas numpy pyarrow

# Databases
pip install --quiet psycopg2-binary sqlalchemy redis

# Messaging
pip install --quiet confluent-kafka kafka-python

# Storage
pip install --quiet boto3 minio

# Monitoring
pip install --quiet prometheus-client python-json-logger

# Data Quality
pip install --quiet great-expectations pandera

# Geospatial
pip install --quiet shapely geopandas geopy

# Testing
pip install --quiet pytest pytest-cov pytest-asyncio pytest-mock pytest-xdist

# Utilities
pip install --quiet python-dateutil pytz tenacity pybreaker jsonschema

echo "   ✓ Dependências instaladas!"

# ============================================================================
# 6. CRIAR DOCKER COMPOSE
# ============================================================================
echo "🐋 [6/8] Criando docker-compose.yml..."

cat > docker-compose.yml << 'DOCKER_EOF'
version: '3.8'

services:
  # PostgreSQL
  postgres:
    image: postgres:15-alpine
    container_name: sptrans-postgres
    environment:
      POSTGRES_DB: sptrans_test
      POSTGRES_USER: test_user
      POSTGRES_PASSWORD: test_password
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U test_user"]
      interval: 10s
      timeout: 5s
      retries: 5

  # Redis
  redis:
    image: redis:7-alpine
    container_name: sptrans-redis
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 5

  # MinIO (S3-compatible)
  minio:
    image: minio/minio:latest
    container_name: sptrans-minio
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    ports:
      - "9000:9000"
      - "9001:9001"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 10s
      timeout: 5s
      retries: 5
DOCKER_EOF

echo "   ✓ docker-compose.yml criado!"

# ============================================================================
# 7. SUBIR SERVIÇOS DOCKER
# ============================================================================
echo "🚀 [7/8] Iniciando serviços Docker..."
docker-compose up -d

echo "   ⏳ Aguardando serviços ficarem prontos (20 segundos)..."
sleep 20

# ============================================================================
# 8. CRIAR ARQUIVO .ENV
# ============================================================================
echo "📝 [8/8] Criando arquivo .env..."

cat > .env << 'ENV_EOF'
# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=sptrans_test
POSTGRES_USER=test_user
POSTGRES_PASSWORD=test_password

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=

# Kafka (comentado - não habilitado no docker-compose light)
# KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# MinIO
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=sptrans-datalake

# SPTrans API
SPTRANS_API_TOKEN=your_token_here
SPTRANS_API_BASE_URL=http://api.olhovivo.sptrans.com.br/v2.1

# Spark
SPARK_HOME=/opt/spark
JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV_EOF

echo "   ✓ .env criado!"

# ============================================================================
# VERIFICAR STATUS
# ============================================================================
echo ""
echo "═══════════════════════════════════════════════════════════"
echo "✅ SETUP COMPLETO!"
echo "═══════════════════════════════════════════════════════════"
echo ""
echo "📦 Instalado:"
echo "  ✓ Java 11"
echo "  ✓ Python $(python3 --version | cut -d' ' -f2)"
echo "  ✓ PySpark 3.5.0"
echo "  ✓ Pytest + 40 dependências"
echo ""
echo "🐋 Serviços Docker:"
docker-compose ps
echo ""
echo "🎯 Próximos passos:"
echo "  1. Ativar ambiente virtual:"
echo "     source venv/bin/activate"
echo ""
echo "  2. Verificar instalação:"
echo "     python3 -c 'import pyspark; print(pyspark.__version__)'"
echo ""
echo "  3. Executar testes:"
echo "     pytest tests/unit/ -v"
echo ""
echo "═══════════════════════════════════════════════════════════"
echo ""
echo "💡 Dica: Para reativar ambiente depois:"
echo "   source venv/bin/activate"
echo ""

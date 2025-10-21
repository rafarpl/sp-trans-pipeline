# 🚀 Guia Completo de Instalação e Setup

> **Manual passo-a-passo para configurar o ambiente do projeto**  
> **Versão**: 1.0 | **Data**: Outubro 2025

---

## 📋 Índice

1. [Pré-requisitos](#1-pré-requisitos)
2. [Instalação Local](#2-instalação-local)
3. [Configuração de Serviços](#3-configuração-de-serviços)
4. [Validação do Ambiente](#4-validação-do-ambiente)
5. [Troubleshooting](#5-troubleshooting)

---

## 1. Pré-requisitos

### 1.1. Hardware Mínimo

| Componente | Mínimo | Recomendado | Motivo |
|------------|--------|-------------|--------|
| **CPU** | 4 cores | 8 cores | Spark executors + Airflow workers |
| **RAM** | 16 GB | 32 GB | Spark (12GB) + PostgreSQL (2GB) + outros |
| **Disco** | 100 GB SSD | 250 GB SSD | MinIO storage + Docker images |
| **Network** | 10 Mbps | 50+ Mbps | API SPTrans downloads |

### 1.2. Software Necessário

**Sistema Operacional:**
- Linux (Ubuntu 20.04+ ou CentOS 8+) - Recomendado
- macOS 12+ - Suportado
- Windows 10/11 com WSL2 - Suportado

**Ferramentas Obrigatórias:**

```bash
# Docker
docker --version
# Docker version 24.0.0+

# Docker Compose
docker-compose --version
# Docker Compose version 2.20.0+

# Git
git --version
# git version 2.40.0+

# Curl (para testes de API)
curl --version
# curl 7.81.0+
```

**Ferramentas Opcionais (Desenvolvimento):**

```bash
# Python 3.11+ (para desenvolvimento local)
python3 --version

# Make (para comandos úteis)
make --version

# jq (para processar JSON)
jq --version
```

### 1.3. Credenciais Necessárias

**1. Token API SPTrans:**
- Acesse: https://www.sptrans.com.br/desenvolvedores/
- Cadastre-se e aguarde aprovação (24-48h)
- Guarde o token recebido

**2. Email SMTP (Opcional - para alertas):**
- Gmail, Outlook ou SMTP customizado
- Username e password

---

## 2. Instalação Local

### 2.1. Clone do Repositório

```bash
# Clone do projeto
git clone https://github.com/rafarpl/sp-trans-pipeline.git
cd sp-trans-pipeline

# Verifique a estrutura
ls -la
# Deve mostrar: docker-compose.yml, .env.example, src/, dags/, etc.
```

### 2.2. Configuração de Variáveis de Ambiente

```bash
# Copie o template
cp .env.example .env

# Edite com suas credenciais
nano .env  # ou vim, code, etc.
```

**Conteúdo do .env:**

```bash
# ========================================
# SPTRANS API
# ========================================
SPTRANS_API_TOKEN=seu_token_aqui_obtido_no_portal_sptrans
SPTRANS_API_BASE_URL=https://api.olhovivo.sptrans.com.br/v2.1

# ========================================
# MINIO (Data Lake)
# ========================================
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin123  # ALTERAR EM PRODUÇÃO
MINIO_ENDPOINT=minio:9000
MINIO_CONSOLE_PORT=9001
MINIO_BUCKET_BRONZE=sptrans-bronze
MINIO_BUCKET_SILVER=sptrans-silver
MINIO_BUCKET_GOLD=sptrans-gold

# ========================================
# POSTGRESQL (Data Warehouse)
# ========================================
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=sptrans
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow123  # ALTERAR EM PRODUÇÃO
POSTGRES_SCHEMA_SERVING=serving
POSTGRES_SCHEMA_METADATA=metadata

# ========================================
# REDIS (Cache)
# ========================================
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_PASSWORD=redis123  # ALTERAR EM PRODUÇÃO
REDIS_DB=0

# ========================================
# AIRFLOW
# ========================================
AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow123@postgres:5432/airflow
AIRFLOW__CELERY__BROKER_URL=redis://redis:6379/0
AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow:airflow123@postgres:5432/airflow
AIRFLOW__CORE__FERNET_KEY=ZmDfcTF7_60GrrY167zsiPd67pEvs0aGOv2oasOM1Pg=  # GERAR NOVO
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
AIRFLOW__CORE__LOAD_EXAMPLES=false
AIRFLOW__WEBSERVER__SECRET_KEY=sua_secret_key_aqui  # GERAR NOVO
AIRFLOW_UID=50000

# ========================================
# SPARK
# ========================================
SPARK_MASTER_HOST=spark-master
SPARK_MASTER_PORT=7077
SPARK_MASTER_WEBUI_PORT=8081
SPARK_WORKER_CORES=2
SPARK_WORKER_MEMORY=4g
SPARK_EXECUTOR_MEMORY=4g
SPARK_DRIVER_MEMORY=2g

# ========================================
# SUPERSET (BI)
# ========================================
SUPERSET_SECRET_KEY=sua_superset_secret_key  # GERAR NOVO
SUPERSET_ADMIN_USERNAME=admin
SUPERSET_ADMIN_PASSWORD=admin123  # ALTERAR EM PRODUÇÃO
SUPERSET_ADMIN_EMAIL=admin@sptrans.local

# ========================================
# GRAFANA (Monitoring)
# ========================================
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=admin123  # ALTERAR EM PRODUÇÃO

# ========================================
# PROMETHEUS
# ========================================
PROMETHEUS_PORT=9090

# ========================================
# ALERTING (Opcional)
# ========================================
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=seu_email@gmail.com
SMTP_PASSWORD=sua_senha_app
ALERT_EMAIL=alert@example.com

# ========================================
# ENVIRONMENT
# ========================================
ENVIRONMENT=development  # development | staging | production
LOG_LEVEL=INFO  # DEBUG | INFO | WARNING | ERROR
TZ=America/Sao_Paulo
```

**Gerar chaves secretas:**

```bash
# Gerar Fernet Key para Airflow
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Gerar Secret Key para Superset
openssl rand -base64 42

# Gerar Secret Key para Airflow Webserver
openssl rand -hex 16
```

### 2.3. Preparar Estrutura de Diretórios

```bash
# Criar diretórios necessários
mkdir -p data/gtfs
mkdir -p data/samples
mkdir -p data/backups
mkdir -p logs/{airflow,spark,application}
mkdir -p config/spark
mkdir -p config/prometheus
mkdir -p config/grafana/dashboards

# Definir permissões (importante!)
chmod -R 755 data/
chmod -R 755 logs/
chmod -R 755 config/

# Airflow precisa de UID específico
sudo chown -R 50000:50000 logs/airflow
```

### 2.4. Download de Dados GTFS

```bash
# Script automático
python3 scripts/download_gtfs.py

# OU manualmente
cd data/gtfs/
wget https://www.sptrans.com.br/umbraco/surface/GTFS/ReadFeed -O gtfs.zip
unzip gtfs.zip
rm gtfs.zip
cd ../..

# Verificar arquivos
ls -lh data/gtfs/
# Deve conter: routes.txt, trips.txt, stops.txt, stop_times.txt, shapes.txt
```

### 2.5. Build e Start dos Containers

```bash
# Build de imagens customizadas (primeira vez)
docker-compose build

# Iniciar todos os serviços
docker-compose up -d

# Aguardar inicialização (2-3 minutos)
echo "Aguardando serviços iniciarem..."
sleep 180

# Verificar status
docker-compose ps
```

**Output esperado:**

```
NAME                 STATUS        PORTS
minio                Up            0.0.0.0:9000->9000/tcp, 0.0.0.0:9001->9001/tcp
postgres             Up            0.0.0.0:5432->5432/tcp
redis                Up            0.0.0.0:6379->6379/tcp
airflow-webserver    Up            0.0.0.0:8080->8080/tcp
airflow-scheduler    Up            
airflow-worker       Up            
spark-master         Up            0.0.0.0:7077->7077/tcp, 0.0.0.0:8081->8081/tcp
spark-worker-1       Up            
spark-worker-2       Up            
superset             Up            0.0.0.0:8088->8088/tcp
grafana              Up            0.0.0.0:3000->3000/tcp
prometheus           Up            0.0.0.0:9090->9090/tcp
```

---

## 3. Configuração de Serviços

### 3.1. Inicializar MinIO (Data Lake)

```bash
# Acessar container MinIO
docker-compose exec minio mc config host add minio http://localhost:9000 minioadmin minioadmin123

# Criar buckets
docker-compose exec minio mc mb minio/sptrans-bronze
docker-compose exec minio mc mb minio/sptrans-silver
docker-compose exec minio mc mb minio/sptrans-gold

# Configurar políticas de acesso
docker-compose exec minio mc anonymous set download minio/sptrans-bronze
docker-compose exec minio mc anonymous set download minio/sptrans-silver
docker-compose exec minio mc anonymous set download minio/sptrans-gold

# Verificar buckets criados
docker-compose exec minio mc ls minio/
```

**Acessar Console MinIO:**
- URL: http://localhost:9001
- User: minioadmin
- Pass: minioadmin123

### 3.2. Inicializar PostgreSQL

```bash
# Executar scripts de setup
docker-compose exec postgres psql -U airflow -d postgres -f /sql/00_create_databases.sql
docker-compose exec postgres psql -U airflow -d sptrans -f /sql/01_serving_schema.sql
docker-compose exec postgres psql -U airflow -d sptrans -f /sql/02_serving_tables.sql
docker-compose exec postgres psql -U airflow -d sptrans -f /sql/03_materialized_views.sql
docker-compose exec postgres psql -U airflow -d sptrans -f /sql/04_indexes.sql

# Verificar schemas criados
docker-compose exec postgres psql -U airflow -d sptrans -c "\dn"
# Deve listar: serving, metadata

# Verificar tabelas
docker-compose exec postgres psql -U airflow -d sptrans -c "\dt serving.*"
```

**Conectar via CLI:**

```bash
# Dentro do container
docker-compose exec postgres psql -U airflow -d sptrans

# De fora (se PostgreSQL client instalado)
psql -h localhost -p 5432 -U airflow -d sptrans
```

### 3.3. Inicializar Airflow

```bash
# Criar usuário admin
docker-compose exec airflow-webserver airflow users create \
    --username admin \
    --password admin123 \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@sptrans.local

# Listar DAGs
docker-compose exec airflow-scheduler airflow dags list

# Verificar conexões
docker-compose exec airflow-scheduler airflow connections list
```

**Criar Connections no Airflow:**

```bash
# MinIO (S3) Connection
docker-compose exec airflow-scheduler airflow connections add 'minio_s3' \
    --conn-type 's3' \
    --conn-extra '{
        "aws_access_key_id": "minioadmin",
        "aws_secret_access_key": "minioadmin123",
        "host": "http://minio:9000"
    }'

# PostgreSQL Connection
docker-compose exec airflow-scheduler airflow connections add 'postgres_sptrans' \
    --conn-type 'postgres' \
    --conn-host 'postgres' \
    --conn-schema 'sptrans' \
    --conn-login 'airflow' \
    --conn-password 'airflow123' \
    --conn-port 5432

# Spark Connection
docker-compose exec airflow-scheduler airflow connections add 'spark_default' \
    --conn-type 'spark' \
    --conn-host 'spark://spark-master' \
    --conn-port 7077
```

**Criar Variables no Airflow:**

```bash
# API Token
docker-compose exec airflow-scheduler airflow variables set \
    SPTRANS_API_TOKEN "seu_token_aqui"

# Configurações
docker-compose exec airflow-scheduler airflow variables set \
    MINIO_ENDPOINT "minio:9000"

docker-compose exec airflow-scheduler airflow variables set \
    ENVIRONMENT "development"
```

**Acessar Web UI:**
- URL: http://localhost:8080
- User: admin
- Pass: admin123

### 3.4. Configurar Spark

```bash
# Verificar Spark Master
curl http://localhost:8081

# Testar job simples
docker-compose exec spark-master spark-submit \
    --class org.apache.spark.examples.SparkPi \
    --master spark://spark-master:7077 \
    /opt/spark/examples/jars/spark-examples*.jar 10
```

**Acessar Spark UI:**
- URL: http://localhost:8081
- Mostra workers conectados e aplicações

### 3.5. Configurar Superset

```bash
# Inicializar database
docker-compose exec superset superset db upgrade

# Criar usuário admin
docker-compose exec superset superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@sptrans.local \
    --password admin123

# Inicializar Superset
docker-compose exec superset superset init
```

**Adicionar Database Connection:**

1. Acesse: http://localhost:8088
2. Login: admin / admin123
3. Settings → Database Connections → + Database
4. Selecione: PostgreSQL
5. Preencha:
   - Host: postgres
   - Port: 5432
   - Database: sptrans
   - User: airflow
   - Password: airflow123
6. Test Connection → Save

### 3.6. Configurar Grafana

**Acessar Grafana:**
- URL: http://localhost:3000
- User: admin
- Pass: admin123

**Adicionar Datasources:**

1. **PostgreSQL:**
   - Configuration → Data Sources → Add data source
   - Select: PostgreSQL
   - Host: postgres:5432
   - Database: sptrans
   - User: airflow
   - Password: airflow123
   - SSL Mode: disable
   - Save & Test

2. **Prometheus:**
   - Configuration → Data Sources → Add data source
   - Select: Prometheus
   - URL: http://prometheus:9090
   - Save & Test

**Importar Dashboards:**

```bash
# Copiar dashboards para container
docker cp dashboards/grafana/pipeline_monitoring.json grafana:/tmp/
docker cp dashboards/grafana/data_quality.json grafana:/tmp/

# Ou via UI: Dashboards → Import → Upload JSON file
```

---

## 4. Validação do Ambiente

### 4.1. Health Checks

```bash
# Script automatizado
bash scripts/health_check.sh
```

**OU manualmente:**

```bash
# MinIO
curl -I http://localhost:9001/
# HTTP/1.1 200 OK

# PostgreSQL
docker-compose exec postgres pg_isready -U airflow
# postgres:5432 - accepting connections

# Redis
docker-compose exec redis redis-cli ping
# PONG

# Airflow
curl http://localhost:8080/health
# {"metadatabase":{"status":"healthy"}}

# Spark
curl http://localhost:8081
# Spark Master UI HTML

# Superset
curl -I http://localhost:8088/health
# HTTP/1.1 200 OK

# Grafana
curl -I http://localhost:3000/api/health
# HTTP/1.1 200 OK

# Prometheus
curl http://localhost:9090/-/healthy
# Prometheus is Healthy
```

### 4.2. Teste de Ingestão

```bash
# Trigger DAG de teste
docker-compose exec airflow-scheduler airflow dags trigger dag_01_gtfs_ingestion

# Monitorar execução
docker-compose exec airflow-scheduler airflow dags list-runs -d dag_01_gtfs_ingestion

# Ver logs
docker-compose logs -f airflow-scheduler
```

### 4.3. Verificar Dados Ingeridos

**MinIO:**

```bash
# Listar objetos
docker-compose exec minio mc ls --recursive minio/sptrans-bronze/

# Deve mostrar arquivos Parquet
```

**PostgreSQL:**

```bash
# Contar registros
docker-compose exec postgres psql -U airflow -d sptrans -c \
    "SELECT COUNT(*) FROM serving.kpis_realtime;"
```

### 4.4. Teste End-to-End

```bash
# Script de teste completo
python3 tests/integration/test_end_to_end.py

# OU usando pytest
docker-compose exec airflow-worker pytest tests/integration/ -v
```

---

## 5. Troubleshooting

### 5.1. Problemas Comuns

**Problema: Container não inicia**

```bash
# Ver logs do container
docker-compose logs [service_name]

# Exemplo
docker-compose logs airflow-scheduler

# Restart específico
docker-compose restart airflow-scheduler
```

**Problema: Out of Memory**

```bash
# Verificar uso de recursos
docker stats

# Aumentar memória no docker-compose.yml
services:
  spark-worker:
    mem_limit: 8g  # Aumentar de 4g para 8g
```

**Problema: Porta já em uso**

```bash
# Verificar portas em uso
sudo lsof -i :8080
sudo lsof -i :5432

# Matar processo
sudo kill -9 [PID]

# OU mudar porta no docker-compose.yml
```

**Problema: Permissões negadas**

```bash
# Ajustar ownership
sudo chown -R $USER:$USER .
sudo chown -R 50000:50000 logs/airflow

# Ajustar permissões
chmod -R 755 logs/
chmod -R 755 data/
```

### 5.2. Reset Completo

```bash
# ATENÇÃO: Remove TODOS os dados!

# Parar containers
docker-compose down -v

# Remover volumes
docker volume prune -f

# Limpar dados locais
rm -rf logs/*
rm -rf data/backups/*

# Reiniciar do zero
docker-compose up -d
```

### 5.3. Logs Úteis

```bash
# Todos os logs
docker-compose logs -f

# Log específico
docker-compose logs -f airflow-scheduler

# Últimas 100 linhas
docker-compose logs --tail=100 spark-master

# Logs do Airflow (dentro do container)
docker-compose exec airflow-scheduler cat /opt/airflow/logs/scheduler/latest/*.log
```

### 5.4. Comandos Úteis

```bash
# Ver consumo de recursos
docker stats

# Entrar no container
docker-compose exec [service] bash

# Copiar arquivo para container
docker cp arquivo.txt [container]:/path/

# Copiar arquivo de container
docker cp [container]:/path/arquivo.txt ./

# Reiniciar serviço específico
docker-compose restart [service]

# Ver networks
docker network ls

# Inspecionar network
docker network inspect sptrans-realtime-pipeline_default
```

---

## 6. Próximos Passos

Após setup completo:

1. ✅ **Ativar DAGs no Airflow**
   - dag_01_gtfs_ingestion
   - dag_02_api_ingestion
   - dag_03_bronze_to_silver
   - dag_04_silver_to_gold
   - dag_05_gold_to_serving
   - dag_06_data_quality
   - dag_07_maintenance

2. ✅ **Configurar Dashboards**
   - Importar dashboards no Superset
   - Configurar refresh automático

3. ✅ **Configurar Alertas**
   - Criar alert rules no Grafana
   - Testar notificações por email

4. ✅ **Documentar**
   - Adicionar documentação específica do seu ambiente
   - Documentar customizações feitas

---

**Documento mantido por**: Time de Engenharia de Dados  
**Última atualização**: Outubro 2025  
**Versão**: 1.0
# 🚌 SPTrans Real-Time Data Pipeline

> **Projeto de Conclusão - Pós-Graduação em Engenharia de Dados**  
> **FIA/LABDATA - 2025**

Pipeline completo de dados para monitoramento em tempo real do sistema de transporte público de São Paulo, processando posições de ~15.000 ônibus a cada 2 minutos, com ingestão, processamento distribuído e visualização de KPIs operacionais.

---

## 📋 Índice

- [Visão Geral](#-visão-geral)
- [Arquitetura](#-arquitetura)
- [Tecnologias Utilizadas](#-tecnologias-utilizadas)
- [Estrutura do Projeto](#-estrutura-do-projeto)
- [Pré-requisitos](#-pré-requisitos)
- [Instalação](#-instalação)
- [Configuração](#-configuração)
- [Uso](#-uso)
- [Dashboards e KPIs](#-dashboards-e-kpis)
- [Testes](#-testes)
- [Monitoramento](#-monitoramento)
- [Troubleshooting](#-troubleshooting)
- [Contribuindo](#-contribuindo)
- [Licença](#-licença)

---

## 🎯 Visão Geral

### Objetivo

Criar um produto de dados em **near real-time** que possibilite métricas, KPIs, monitoramento e acompanhamento dos ônibus em circulação no sistema público de transporte da cidade de São Paulo.

### Funcionalidades Principais

- ✅ **Ingestão em Near Real-Time**: Coleta posições de todos os ônibus a cada 2 minutos via API SPTrans Olho Vivo
- ✅ **Processamento Distribuído**: Apache Spark para transformação e agregação de dados massivos
- ✅ **Data Lake Multi-Camada**: Bronze (raw), Silver (cleaned), Gold (aggregated) com Delta Lake
- ✅ **Enriquecimento de Dados**: Cruzamento com GTFS + geocoding reverso (lat/long → endereço)
- ✅ **KPIs Operacionais**: Velocidade média, headway, pontualidade, cobertura de frota
- ✅ **Orquestração Robusta**: Apache Airflow com 7 DAGs para automação completa
- ✅ **Visualização Interativa**: Dashboards em Apache Superset e Grafana
- ✅ **Monitoramento 24/7**: Prometheus + Grafana para observabilidade do pipeline
- ✅ **Qualidade de Dados**: Data quality checks automatizados em cada camada

### Métricas e KPIs Implementados

| KPI | Descrição | Atualização |
|-----|-----------|-------------|
| **Cobertura de Frota** | % de veículos operando vs programados | Tempo real |
| **Velocidade Média** | Por rota, corredor e sistema | A cada 15 min |
| **Headway** | Tempo entre ônibus consecutivos | Tempo real |
| **Pontualidade** | Análise de atrasos/adiantamentos | Horária |
| **Ocupação de Corredor** | Densidade de veículos por via | A cada 5 min |
| **Tempo de Viagem** | Duração real vs programada | Por viagem |
| **Congestionamento** | Identificação de gargalos | Tempo real |

---

## 🏗️ Arquitetura

### Visão Macro

```
API SPTrans → Spark Batch → MinIO (Bronze) → Spark → Delta Lake (Silver/Gold) → PostgreSQL → Dashboards
     ↓                                                        ↓
  GTFS Data                                            Redis Cache
     ↓                                                        ↓
Apache Airflow (Orquestração) ← Prometheus + Grafana (Monitoramento)
```

### Camadas do Pipeline

1. **Data Sources**
   - API SPTrans Olho Vivo (posições em tempo real)
   - GTFS SPTrans (dados estáticos: rotas, paradas, horários)

2. **Ingestion Layer**
   - Apache Spark Batch Jobs (orquestrados por Airflow)
   - Coleta a cada 2 minutos
   - Validação de schema na entrada

3. **Storage Layer (Data Lake)**
   - **MinIO** (S3-compatible object storage)
   - **Bronze**: Raw data em Parquet (particionado por data)
   - **Silver**: Cleaned data com Delta Lake (deduplicado, validado, enriquecido)
   - **Gold**: Aggregated data com Delta Lake (KPIs, métricas agregadas)

4. **Processing Layer**
   - **Apache Spark** para todas as transformações
   - Jobs batch orquestrados por Airflow
   - Data quality checks em cada transformação

5. **Serving Layer**
   - **PostgreSQL**: Data Warehouse com dados agregados para dashboards
   - **Redis**: Cache para dados de tempo real (TTL 2 minutos)

6. **Presentation Layer**
   - **Apache Superset**: Dashboards interativos e análises
   - **Grafana**: Monitoramento do sistema e KPIs operacionais

7. **Orchestration & Monitoring**
   - **Apache Airflow**: Orquestração de todos os jobs
   - **Prometheus + Grafana**: Métricas e alertas do pipeline

### Fluxo de Dados Detalhado

Ver documentação completa em: [`docs/01_architecture.md`](docs/01_architecture.md)

---

## 🛠️ Tecnologias Utilizadas

### Core Stack (100% Open Source)

| Categoria | Tecnologia | Versão | Justificativa |
|-----------|-----------|---------|---------------|
| **Orquestração** | Apache Airflow | 2.8.0 | Padrão de mercado, scheduler robusto |
| **Processing** | Apache Spark | 3.5.0 | Processamento distribuído, alta performance |
| **Storage** | MinIO | RELEASE.2024 | S3-compatible, escalável, baixo custo |
| **Table Format** | Delta Lake | 3.0.0 | ACID transactions, time travel |
| **Data Warehouse** | PostgreSQL | 16.0 | Serving layer otimizado para OLAP |
| **Cache** | Redis | 7.2 | Cache de baixa latência para real-time |
| **Visualization** | Apache Superset | 3.0 | BI moderno e flexível |
| **Monitoring** | Prometheus | 2.48 | Métricas e alertas |
| **Monitoring** | Grafana | 10.2 | Dashboards de observabilidade |
| **Message Queue** | Apache Kafka | 3.6 | (Opcional) Streaming real-time |

### Linguagens e Frameworks

- **Python 3.11**: Linguagem principal
- **PySpark**: Processamento distribuído
- **SQL**: Queries e transformações
- **Docker**: Containerização
- **Docker Compose**: Orquestração local

---

## 📁 Estrutura do Projeto

```
sptrans-realtime-pipeline/
├── 📂 docs/              # Documentação completa
├── 📂 infra/             # Docker, Kubernetes, Terraform
├── 📂 config/            # Configurações de serviços
├── 📂 sql/               # Scripts SQL (serving layer)
├── 📂 src/               # Código-fonte Python
│   ├── common/           # Utils, config, logging
│   ├── ingestion/        # Clientes API e schemas
│   ├── processing/       # Spark jobs e transformações
│   ├── serving/          # Load para PostgreSQL/Redis
│   └── monitoring/       # Health checks e alertas
├── 📂 dags/              # DAGs do Airflow
├── 📂 tests/             # Testes unitários e integração
├── 📂 scripts/           # Scripts auxiliares
├── 📂 notebooks/         # Jupyter notebooks (EDA)
└── 📂 dashboards/        # Dashboards exportados
```

Ver estrutura completa: [`docs/02_setup_guide.md`](docs/02_setup_guide.md)

---

## 📋 Pré-requisitos

### Hardware Mínimo

- **CPU**: 4+ cores (8+ recomendado)
- **RAM**: 16GB (32GB recomendado para Spark)
- **Storage**: 100GB SSD
- **Network**: 10Mbps+ (API SPTrans)

### Software

- **Docker** 24.0+
- **Docker Compose** 2.20+
- **Git** 2.40+
- **Python** 3.11+ (para desenvolvimento local)
- **Make** (opcional, para comandos úteis)

### Credenciais Necessárias

1. **Token API SPTrans**: Solicitar em https://www.sptrans.com.br/desenvolvedores/
2. **SMTP** (opcional): Para envio de alertas por email

---

## 🚀 Instalação

### 1. Clone o Repositório

```bash
git clone https://github.com/rafarpl/sp-trans-pipeline.git
cd sp-trans-pipeline
```

### 2. Configure Variáveis de Ambiente

```bash
cp .env.example .env
nano .env  # Edite com suas credenciais
```

**Variáveis obrigatórias:**
```bash
SPTRANS_API_TOKEN=seu_token_aqui
POSTGRES_PASSWORD=senha_segura
```

### 3. Inicie os Serviços

```bash
# Usando Make (recomendado)
make setup

# OU manualmente
docker-compose up -d
```

Isso irá iniciar:
- ✅ MinIO (Data Lake)
- ✅ PostgreSQL (Data Warehouse)
- ✅ Redis (Cache)
- ✅ Airflow (Webserver, Scheduler, Workers)
- ✅ Spark (Master + 2 Workers)
- ✅ Superset (Dashboards)
- ✅ Prometheus + Grafana (Monitoring)

### 4. Verifique Status dos Serviços

```bash
make status

# OU
docker-compose ps
```

**URLs de Acesso:**
- **Airflow**: http://localhost:8080 (admin/admin)
- **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin)
- **Superset**: http://localhost:8088 (admin/admin)
- **Grafana**: http://localhost:3000 (admin/admin)
- **Prometheus**: http://localhost:9090

---

## ⚙️ Configuração

### 1. Inicializar Data Lake (MinIO)

```bash
# Criar buckets necessários
make init-minio

# OU manualmente
docker-compose exec minio mc mb minio/sptrans-bronze
docker-compose exec minio mc mb minio/sptrans-silver
docker-compose exec minio mc mb minio/sptrans-gold
```

### 2. Criar Schemas no PostgreSQL

```bash
# Executar todos os scripts SQL
make init-postgres

# OU manualmente
docker-compose exec postgres psql -U airflow -f /sql/00_create_databases.sql
docker-compose exec postgres psql -U airflow -d sptrans -f /sql/01_serving_schema.sql
```

### 3. Baixar Dados GTFS

```bash
# Download automático
make download-gtfs

# OU manualmente
python scripts/download_gtfs.py
```

### 4. Ativar DAGs no Airflow

Acesse http://localhost:8080 e ative:

1. ✅ `gtfs_ingestion_daily` - Carga diária GTFS (3h AM)
2. ✅ `api_ingestion_realtime` - Ingestão API (a cada 2 min)
3. ✅ `bronze_to_silver` - Limpeza e validação (a cada 10 min)
4. ✅ `silver_to_gold` - Agregações (a cada 15 min)
5. ✅ `gold_to_serving` - Load PostgreSQL (a cada 15 min)
6. ✅ `data_quality_monitoring` - DQ checks (horária)
7. ✅ `maintenance_cleanup` - Limpeza (diária, 2h AM)

---

## 💻 Uso

### Executar Ingestão Manual

```bash
# Trigger DAG via CLI
docker-compose exec airflow-scheduler airflow dags trigger api_ingestion_realtime

# OU via UI
# Acesse http://localhost:8080 → DAGs → Play button
```

### Consultar Dados no Data Lake

```python
# Notebook Jupyter ou Spark Shell
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SPTrans Analysis") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .getOrCreate()

# Ler camada Bronze
df_bronze = spark.read.parquet("s3a://sptrans-bronze/api_positions/")
df_bronze.show()

# Ler camada Silver (Delta Lake)
df_silver = spark.read.format("delta").load("s3a://sptrans-silver/positions_cleaned/")
df_silver.show()

# Ler camada Gold (agregado)
df_gold = spark.read.format("delta").load("s3a://sptrans-gold/kpis_hourly/")
df_gold.show()
```

### Consultar Dados no PostgreSQL

```sql
-- Conectar ao PostgreSQL
docker-compose exec postgres psql -U airflow -d sptrans

-- Ver KPIs em tempo real
SELECT * FROM serving.kpis_realtime ORDER BY updated_at DESC LIMIT 10;

-- Velocidade média por rota
SELECT 
    route_short_name,
    AVG(speed_kmh) as avg_speed,
    COUNT(*) as samples
FROM serving.route_metrics
WHERE timestamp >= NOW() - INTERVAL '1 hour'
GROUP BY route_short_name
ORDER BY avg_speed DESC;
```

### Acessar Cache Redis

```bash
# Conectar ao Redis
docker-compose exec redis redis-cli

# Ver posições em cache
KEYS bus:position:*
GET bus:position:12345

# Ver estatísticas
INFO stats
```

---

## 📊 Dashboards e KPIs

### Apache Superset

Acesse: http://localhost:8088

**Dashboards Disponíveis:**

1. **Operational Dashboard**
   - Total de veículos ativos
   - Mapa de posições em tempo real
   - Velocidade média por corredor
   - Alertas ativos

2. **Executive Summary**
   - KPIs principais (cards)
   - Tendências semanais
   - Comparativos dia útil vs fim de semana

3. **Route Performance**
   - Ranking de rotas por pontualidade
   - Headway médio por linha
   - Análise de desvios

### Grafana (Monitoring)

Acesse: http://localhost:3000

**Dashboards:**

1. **Pipeline Monitoring**
   - Taxa de sucesso dos DAGs
   - Latência de ingestão
   - Throughput de processamento
   - Erros e retries

2. **Data Quality**
   - % de registros válidos
   - Duplicatas detectadas
   - Outliers identificados
   - Completude dos dados

---

## 🧪 Testes

### Executar Todos os Testes

```bash
# Usando Make
make test

# OU manualmente
docker-compose exec airflow-worker pytest tests/ -v
```

### Testes Unitários

```bash
pytest tests/unit/ -v
```

### Testes de Integração

```bash
pytest tests/integration/ -v
```

### Cobertura de Testes

```bash
make test-coverage

# Gera relatório HTML em htmlcov/index.html
```

---

## 📈 Monitoramento

### Health Checks

```bash
# Via script
make health-check

# OU manualmente
curl http://localhost:8080/health  # Airflow
curl http://localhost:9000/minio/health/live  # MinIO
```

### Logs

```bash
# Airflow logs
docker-compose logs -f airflow-scheduler

# Spark logs
docker-compose logs -f spark-master

# Todos os serviços
docker-compose logs -f
```

### Métricas Prometheus

Acesse: http://localhost:9090

**Queries úteis:**
```promql
# Taxa de sucesso de ingestão
rate(airflow_dag_run_success_total{dag_id="api_ingestion_realtime"}[5m])

# Latência de processamento
histogram_quantile(0.95, rate(spark_job_duration_seconds_bucket[5m]))

# Erros no pipeline
sum(rate(pipeline_errors_total[5m])) by (component)
```

---

## 🔧 Troubleshooting

### Problemas Comuns

| Problema | Solução |
|----------|---------|
| **DAG não aparece no Airflow** | Verificar logs: `docker-compose logs airflow-scheduler` |
| **Erro de conexão MinIO** | Verificar credenciais no `.env` e restart: `make restart-minio` |
| **Spark job travado** | Verificar recursos: `docker stats` e aumentar memória se necessário |
| **API SPTrans timeout** | Verificar token válido e conectividade: `curl -H "Authorization: Bearer $TOKEN" https://api.olhovivo.sptrans.com.br/v2.1/Posicao` |

Ver guia completo: [`docs/05_troubleshooting.md`](docs/05_troubleshooting.md)

---

## 🤝 Contribuindo

Contribuições são bem-vindas! Para contribuir:

1. Fork o projeto
2. Crie uma branch para sua feature (`git checkout -b feature/MinhaFeature`)
3. Commit suas mudanças (`git commit -m 'Adiciona MinhaFeature'`)
4. Push para a branch (`git push origin feature/MinhaFeature`)
5. Abra um Pull Request

### Guidelines

- Siga PEP 8 para código Python
- Adicione testes para novas funcionalidades
- Atualize documentação quando necessário
- Use commits semânticos (feat, fix, docs, etc)

---

## 📄 Licença

Este projeto está sob a licença MIT. Veja [LICENSE](LICENSE) para mais detalhes.

---

## 👥 Autores

**Rafael** - Projeto de Conclusão FIA/LABDATA  
GitHub: [@rafarpl](https://github.com/rafarpl)

---

## 🙏 Agradecimentos

- **SPTrans** pela disponibilização da API Olho Vivo
- **FIA/LABDATA** pelo programa de Engenharia de Dados
- Comunidades **Apache Airflow**, **Spark**, **Delta Lake**
- Todos os contribuidores do ecossistema open source

---

## 📚 Documentação Adicional

- [Arquitetura Detalhada](docs/01_architecture.md)
- [Guia de Setup](docs/02_setup_guide.md)
- [Dicionário de Dados](docs/03_data_dictionary.md)
- [Referência API SPTrans](docs/04_api_reference.md)
- [Troubleshooting](docs/05_troubleshooting.md)
- [Justificativas Técnicas](docs/06_justifications.md)
- [Catálogo de Metadados](docs/07_metadata_catalog.md)

---

**🚀 Desenvolvido com ❤️ para melhorar o transporte público de São Paulo**

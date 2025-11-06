# 🚍 SPTrans Real-Time Data Pipeline

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-3.5.0-orange.svg)](https://spark.apache.org/)
[![Airflow](https://img.shields.io/badge/Airflow-2.8.0-red.svg)](https://airflow.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.0-green.svg)](https://delta.io/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> **Pipeline de dados em tempo real para monitoramento e análise da frota de ônibus da SPTrans em São Paulo**

Projeto de TCC - Pós-Graduação em Engenharia de Dados | FIA/LABDATA 2024

---

## 📋 Índice

- [Sobre o Projeto](#-sobre-o-projeto)
- [Arquitetura](#-arquitetura)
- [Features](#-features)
- [Tecnologias](#-tecnologias)
- [Pré-requisitos](#-pré-requisitos)
- [Instalação](#-instalação)
- [Uso](#-uso)
- [KPIs e Métricas](#-kpis-e-métricas)
- [Documentação](#-documentação)
- [Estrutura do Projeto](#-estrutura-do-projeto)
- [Contribuição](#-contribuição)
- [Licença](#-licença)
- [Autor](#-autor)

---

## 🎯 Sobre o Projeto

Este projeto implementa um **pipeline de dados em tempo real** para coletar, processar e analisar dados de GPS de aproximadamente **15.000 veículos** da frota de ônibus da SPTrans (São Paulo).

### Objetivos

- ✅ Ingestão de dados em **near real-time** (< 3 minutos)
- ✅ Processamento distribuído com **Apache Spark**
- ✅ Arquitetura **Medallion** (Bronze → Silver → Gold → Serving)
- ✅ Integração com dados **GTFS** (rotas, horários, paradas)
- ✅ Cálculo de **KPIs** de operação e qualidade de serviço
- ✅ Dashboards interativos para monitoramento
- ✅ 100% **Open Source**

### Problema de Negócio

A SPTrans disponibiliza dados de posicionamento de sua frota via API pública, mas:
- Dados não são historicizados
- Não há análises de qualidade de serviço
- Falta integração com informações de rotas (GTFS)
- Sem métricas operacionais consolidadas

**Este projeto resolve** estes problemas com uma solução escalável e moderna.

---

## 🏗️ Arquitetura

### Visão Geral

```
┌─────────────────┐
│   DATA SOURCES  │
├─────────────────┤
│  SPTrans API    │──┐
│  (15k vehicles) │  │
│  GTFS Static    │  │
└─────────────────┘  │
                     │
┌────────────────────▼──────────────────────┐
│           INGESTION LAYER                 │
├───────────────────────────────────────────┤
│  • API Client (Circuit Breaker)           │
│  • Kafka Producer (real-time streaming)   │
│  • GTFS Downloader (batch)                │
└────────────────────┬──────────────────────┘
                     │
┌────────────────────▼──────────────────────┐
│         PROCESSING LAYER (Spark)          │
├───────────────────────────────────────────┤
│  BRONZE (Raw Data - MinIO)                │
│    • vehicle_positions                     │
│    • gtfs_static                           │
│                                            │
│  SILVER (Cleaned & Validated)             │
│    • Deduplication                         │
│    • Data Quality Checks                   │
│    • Schema Enforcement                    │
│                                            │
│  GOLD (Business Logic)                    │
│    • GTFS Integration                      │
│    • Geocoding (reverse)                   │
│    • Aggregations                          │
│                                            │
│  SERVING (PostgreSQL)                     │
│    • Materialized Views                    │
│    • KPI Tables                            │
│    • Time-series Aggregates                │
└────────────────────┬──────────────────────┘
                     │
┌────────────────────▼──────────────────────┐
│         ORCHESTRATION (Airflow)           │
├───────────────────────────────────────────┤
│  DAG 1: GTFS Ingestion (daily)            │
│  DAG 2: API Ingestion (every 3 min)       │
│  DAG 3: Bronze → Silver (streaming)       │
│  DAG 4: Silver → Gold (batch)             │
│  DAG 5: Gold → Serving (batch)            │
│  DAG 6: Data Quality Checks               │
│  DAG 7: Maintenance & Optimization        │
└────────────────────┬──────────────────────┘
                     │
┌────────────────────▼──────────────────────┐
│      MONITORING & OBSERVABILITY           │
├───────────────────────────────────────────┤
│  • Prometheus (metrics)                    │
│  • Grafana (dashboards)                    │
│  • Data Quality Alerts                     │
│  • Structured Logging                      │
└───────────────────────────────────────────┘
```

### Arquitetura Medallion

| Layer | Descrição | Storage | Formato | Retention |
|-------|-----------|---------|---------|-----------|
| **Bronze** | Raw data (imutável) | MinIO (S3) | Parquet | 90 dias |
| **Silver** | Cleaned & validated | MinIO (S3) | Delta Lake | 180 dias |
| **Gold** | Business aggregations | MinIO (S3) | Delta Lake | 1 ano |
| **Serving** | Materialized views | PostgreSQL | Tables/Views | Indefinido |

---

## ✨ Features

### Ingestão de Dados
- ✅ **API SPTrans**: Polling a cada 3 minutos (15.000 veículos)
- ✅ **Circuit Breaker**: Proteção contra falhas de API
- ✅ **Kafka Streaming**: Ingestão em tempo real
- ✅ **GTFS Integration**: Download automático de rotas e horários

### Processamento
- ✅ **Spark Streaming**: Processamento incremental
- ✅ **Delta Lake**: ACID transactions + time travel
- ✅ **Data Quality**: Great Expectations + Pandera
- ✅ **Deduplication**: Remoção de duplicatas
- ✅ **Enrichment**: Geocoding reverso + integração GTFS

### KPIs Calculados
- 📊 **Cobertura da Frota**: % veículos em operação
- 🚌 **Frota Ativa**: Veículos transmitindo
- ⏱️ **Velocidade Média**: Por linha e período
- 📍 **Headway**: Intervalo entre veículos
- 🚦 **Pontualidade**: Desvio vs horário programado
- 🗺️ **Heatmaps**: Concentração de veículos

### Observabilidade
- 📈 **Prometheus**: Métricas técnicas
- 📊 **Grafana**: 4 dashboards (pipeline, DQ, business, system)
- 🔔 **Alertas**: Slack/Email para anomalias
- 📝 **Logs Estruturados**: JSON + níveis de severidade

---

## 🛠️ Tecnologias

### Core Stack
| Tecnologia | Versão | Uso |
|------------|--------|-----|
| **Python** | 3.9+ | Linguagem principal |
| **Apache Spark** | 3.5.0 | Processamento distribuído |
| **Delta Lake** | 3.0.0 | Data lakehouse |
| **Apache Kafka** | 3.6 | Streaming real-time |
| **PostgreSQL** | 15 | Serving layer |
| **MinIO** | RELEASE.2024 | Object storage (S3-compatible) |
| **Apache Airflow** | 2.8.0 | Orquestração |

### Monitoring & DevOps
- **Prometheus** (2.48+): Métricas
- **Grafana** (10.2+): Visualização
- **Docker** + **Docker Compose**: Containerização
- **Kubernetes** (opcional): Deployment
- **Terraform** (opcional): IaC

### Libraries Python
- **pyspark**: Processamento Spark
- **confluent-kafka**: Kafka producer/consumer
- **pandas**: Manipulação de dados
- **great-expectations**: Data quality
- **geopandas**: Análise geoespacial
- **sqlalchemy**: ORM PostgreSQL

---

## 📦 Pré-requisitos

### Software Necessário

```bash
# Obrigatório
- Python 3.9+
- Docker 24.0+ & Docker Compose 2.20+
- Git 2.40+

# Opcional (para deploy)
- Kubernetes 1.28+
- Terraform 1.6+
```

### Hardware Recomendado

| Componente | Mínimo | Recomendado |
|------------|--------|-------------|
| **CPU** | 4 cores | 8+ cores |
| **RAM** | 8 GB | 16+ GB |
| **Disco** | 50 GB | 200+ GB SSD |
| **Rede** | 10 Mbps | 100+ Mbps |

---

## 🚀 Instalação

### 1. Clone o Repositório

```bash
git clone https://github.com/rafarpl/sp-trans-pipeline.git
cd sp-trans-pipeline
```

### 2. Configuração de Ambiente

```bash
# Copiar arquivo de exemplo
cp .env.example .env

# Editar com suas credenciais
nano .env
```

**Variáveis obrigatórias:**
```env
# SPTrans API
SPTRANS_API_TOKEN=your_token_here

# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=sptrans
POSTGRES_USER=sptrans
POSTGRES_PASSWORD=your_password

# MinIO (S3)
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=sptrans-datalake

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

### 3. Criar Virtual Environment

```bash
# Criar venv
python3 -m venv venv

# Ativar
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate     # Windows

# Instalar dependências
pip install -r requirements.txt
```

### 4. Subir Infraestrutura (Docker)

```bash
# Subir todos os serviços
docker-compose up -d

# Verificar status
docker-compose ps

# Logs
docker-compose logs -f
```

**Serviços disponíveis:**
- **Airflow**: http://localhost:8080 (admin/admin)
- **Grafana**: http://localhost:3000 (admin/admin)
- **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin)
- **Prometheus**: http://localhost:9090
- **PostgreSQL**: localhost:5432

### 5. Setup do Database

```bash
# Executar scripts SQL
./scripts/setup.sh

# Ou manualmente
psql -h localhost -U sptrans -d sptrans -f sql/bronze/01_bronze_schema.sql
psql -h localhost -U sptrans -d sptrans -f sql/bronze/02_bronze_tables.sql
# ... (todos os scripts)
```

---

## 💻 Uso

### Modo Desenvolvimento

```bash
# Ativar ambiente
source venv/bin/activate

# Rodar ingestion manual
python -m src.ingestion.sptrans_api_client

# Rodar job Spark
spark-submit src/processing/jobs/bronze_to_silver.py

# Testar DAG
airflow dags test dag_01_gtfs_ingestion 2024-01-01
```

### Modo Produção

```bash
# Iniciar todos os serviços
./scripts/start_services.sh

# Habilitar DAGs no Airflow
# Acessar: http://localhost:8080
# Ativar os 7 DAGs na UI

# Monitorar
# Grafana: http://localhost:3000
# Prometheus: http://localhost:9090
```

### Comandos Úteis

```bash
# Backup
./scripts/backup_data.sh

# Restore
./scripts/restore_data.sh

# Gerar dados de teste
python scripts/generate_test_data.py

# Rodar testes
pytest tests/ -v --cov=src

# Limpar ambiente
./scripts/stop_services.sh
docker-compose down -v
```

---

## 📊 KPIs e Métricas

### KPIs de Negócio

| KPI | Descrição | Cálculo | Alvo |
|-----|-----------|---------|------|
| **Fleet Coverage** | % da frota transmitindo | (ativos / total) × 100 | > 95% |
| **Avg Speed** | Velocidade média por linha | Σ(speed) / count | 15-25 km/h |
| **Headway** | Intervalo entre veículos | Δt entre passagens | < 10 min |
| **Punctuality** | Pontualidade vs programado | \|real - scheduled\| | < 5 min |
| **Trip Completion** | % viagens completas | completas / planejadas | > 90% |

### Métricas Técnicas

- **Latência de Ingestão**: < 30 segundos
- **Throughput**: ~5.000 mensagens/min
- **Data Quality Score**: > 95%
- **Pipeline Success Rate**: > 99%
- **Storage Growth**: ~10 GB/dia

---

## 📚 Documentação

Documentação completa disponível em `docs/`:

- **[Arquitetura](docs/01_architecture.md)**: Decisões técnicas e diagramas
- **[Setup Guide](docs/02_setup_guide.md)**: Instalação passo a passo
- **[User Guide](docs/03_user_guide.md)**: Como usar o sistema
- **[API Reference](docs/04_api_reference.md)**: Referência de APIs
- **[Troubleshooting](docs/05_troubleshooting.md)**: Resolução de problemas
- **[Justifications](docs/06_justifications.md)**: Justificativas técnicas
- **[Data Dictionary](docs/03_data_dictionary.md)**: Dicionário de dados

---

## 📁 Estrutura do Projeto

```
sp-trans-pipeline/
├── src/                          # Código-fonte Python
│   ├── common/                   # Módulos compartilhados
│   │   ├── config.py            # Configurações
│   │   ├── logging_config.py    # Logging estruturado
│   │   ├── exceptions.py        # Exceções customizadas
│   │   └── validators.py        # Validações
│   ├── ingestion/               # Camada de ingestão
│   │   ├── sptrans_api_client.py
│   │   ├── kafka_producer.py
│   │   └── gtfs_downloader.py
│   ├── processing/              # Jobs Spark
│   │   ├── jobs/
│   │   │   ├── kafka_to_bronze.py
│   │   │   ├── bronze_to_silver.py
│   │   │   └── ...
│   │   └── transformations/     # Transformações
│   ├── serving/                 # Serving layer
│   └── monitoring/              # Observabilidade
├── dags/                        # DAGs Airflow
│   ├── dag_01_gtfs_ingestion.py
│   ├── dag_02_api_ingestion.py
│   └── ...
├── sql/                         # Scripts SQL
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── serving/
├── tests/                       # Testes
│   ├── unit/
│   └── integration/
├── config/                      # Configurações
│   ├── airflow/
│   ├── spark/
│   └── grafana/
├── infra/                       # Infraestrutura
│   ├── docker/
│   ├── kubernetes/
│   └── terraform/
├── docs/                        # Documentação
├── notebooks/                   # Jupyter notebooks
├── scripts/                     # Scripts utilitários
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## 🤝 Contribuição

Contribuições são bem-vindas! Por favor:

1. Fork o projeto
2. Crie uma branch (`git checkout -b feature/nova-feature`)
3. Commit suas mudanças (`git commit -m 'Add nova feature'`)
4. Push para a branch (`git push origin feature/nova-feature`)
5. Abra um Pull Request

### Padrões de Código

- **Python**: PEP 8 (black + isort)
- **SQL**: Lowercase, underscores
- **Commits**: Conventional Commits
- **Testes**: Coverage > 80%

---

## 📄 Licença

Este projeto está sob a licença MIT. Veja [LICENSE](LICENSE) para mais detalhes.

---

## 👨‍💻 Autor

**Rafael (rafarpl)**  
Pós-Graduação em Engenharia de Dados  
FIA/LABDATA - 2024

📧 Email: [seu-email]  
🔗 LinkedIn: [seu-linkedin]  
🐙 GitHub: [@rafarpl](https://github.com/rafarpl)

---

## 🙏 Agradecimentos

- **SPTrans** pelos dados públicos da API Olho Vivo
- **FIA/LABDATA** pela orientação e suporte
- **Comunidade Open Source** pelas ferramentas incríveis

---

## 📊 Status do Projeto

![Status](https://img.shields.io/badge/Status-Em%20Desenvolvimento-yellow)
![Build](https://img.shields.io/badge/Build-Passing-green)
![Coverage](https://img.shields.io/badge/Coverage-85%25-brightgreen)

**Última atualização**: Novembro 2024

---

## 🔗 Links Úteis

- [SPTrans API Docs](http://www.sptrans.com.br/desenvolvedores/)
- [GTFS Specification](https://gtfs.org/)
- [Apache Spark Docs](https://spark.apache.org/docs/latest/)
- [Delta Lake Docs](https://docs.delta.io/)
- [Airflow Docs](https://airflow.apache.org/docs/)

---

**⭐ Se este projeto foi útil, considere dar uma estrela!**
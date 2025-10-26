# 🚌 SPTrans Data Pipeline

<div align="center">

![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8-red.svg)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-orange.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)

**Pipeline de dados em tempo real para análise do sistema de transporte público de São Paulo**

[Documentação](docs/) · [Arquitetura](docs/01_architecture.md) · [API Reference](docs/04_api_reference.md) · [Troubleshooting](docs/05_troubleshooting.md)

</div>

---

## 📋 Índice

- [Visão Geral](#-visão-geral)
- [Arquitetura](#-arquitetura)
- [Features](#-features)
- [Tecnologias](#-tecnologias)
- [Instalação](#-instalação)
- [Uso](#-uso)
- [Estrutura do Projeto](#-estrutura-do-projeto)
- [DAGs do Airflow](#-dags-do-airflow)
- [Monitoramento](#-monitoramento)
- [Testes](#-testes)
- [Contribuindo](#-contribuindo)
- [Licença](#-licença)

---

## 🎯 Visão Geral

O **SPTrans Data Pipeline** é uma solução completa de engenharia de dados para coletar, processar e analisar dados em tempo real do sistema de transporte público de São Paulo. O pipeline integra dados da API Olho Vivo da SPTrans e arquivos GTFS para fornecer insights operacionais e estratégicos.

### 🎁 Principais Características

- ⚡ **Ingestão em tempo real** - Coleta de posições dos ônibus a cada 2 minutos
- 🏗️ **Arquitetura Lakehouse** - Camadas Bronze, Silver e Gold para processamento estruturado
- 📊 **Dashboards interativos** - Visualizações com Superset e Grafana
- 🔍 **Monitoramento completo** - Métricas, alertas e observabilidade
- 🧪 **Qualidade de dados** - Validação, limpeza e enriquecimento automatizados
- 📈 **KPIs operacionais** - Análise de performance, headway e acessibilidade

---

## 🏛️ Arquitetura

```
┌─────────────────┐
│  API SPTrans    │
│  GTFS Files     │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────────────────────┐
│              INGESTION LAYER                        │
│  ┌─────────────┐          ┌─────────────┐          │
│  │ API Client  │          │ GTFS        │          │
│  │ (Python)    │          │ Downloader  │          │
│  └──────┬──────┘          └──────┬──────┘          │
└─────────┼────────────────────────┼─────────────────┘
          │                        │
          ▼                        ▼
┌─────────────────────────────────────────────────────┐
│         DATA LAKE (MinIO - S3 Compatible)           │
│  ┌──────────────────────────────────────────────┐  │
│  │  BRONZE LAYER (Raw Data)                     │  │
│  │  - api_positions/                            │  │
│  │  - gtfs/routes, stops, trips, shapes         │  │
│  └──────────────┬───────────────────────────────┘  │
│                 │                                   │
│                 ▼                                   │
│  ┌──────────────────────────────────────────────┐  │
│  │  SILVER LAYER (Cleaned & Enriched)          │  │
│  │  - positions_enriched                        │  │
│  │  - Validated, deduplicated, geocoded        │  │
│  └──────────────┬───────────────────────────────┘  │
│                 │                                   │
│                 ▼                                   │
│  ┌──────────────────────────────────────────────┐  │
│  │  GOLD LAYER (Aggregated & Analytics)        │  │
│  │  - kpis_hourly                               │  │
│  │  - metrics_by_route                          │  │
│  │  - headway_analysis                          │  │
│  └──────────────┬───────────────────────────────┘  │
└─────────────────┼───────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────┐
│           SERVING LAYER (PostgreSQL)                │
│  - Materialized Views                               │
│  - Optimized Indexes                                │
│  - Real-time Queries                                │
└─────────────────┬───────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────┐
│        VISUALIZATION & ANALYTICS                    │
│  ┌─────────────┐  ┌─────────────┐  ┌────────────┐  │
│  │  Superset   │  │   Grafana   │  │  Jupyter   │  │
│  │  (BI)       │  │  (Metrics)  │  │  (Analysis)│  │
│  └─────────────┘  └─────────────┘  └────────────┘  │
└─────────────────────────────────────────────────────┘

Orchestration: Apache Airflow
Processing: Apache Spark
Monitoring: Prometheus + Grafana
```

---

## ✨ Features

### 📥 Ingestão de Dados

- **API em Tempo Real**: Coleta de posições dos ônibus a cada 2 minutos
- **Dados Estáticos GTFS**: Download diário de rotas, paradas e horários
- **Resiliente**: Retry automático, tratamento de erros, circuit breaker

### 🔄 Processamento

- **Validação**: Coordenadas, timestamps, IDs de veículos
- **Limpeza**: Deduplicação, normalização, tratamento de nulos
- **Enriquecimento**: Join com dados GTFS, geocoding reverso
- **Agregações**: KPIs por hora/dia/rota, análise de headway

### 📊 Analytics

- **KPIs Operacionais**:
  - Total de veículos ativos
  - Velocidade média e máxima
  - Taxa de movimentação
  - Acessibilidade

- **Análise de Performance**:
  - Headway (intervalo entre ônibus)
  - Regularidade do serviço
  - Distância percorrida
  - Horas de operação

- **Qualidade de Dados**:
  - Score de completude
  - Taxa de validade
  - Alertas automáticos

### 🎯 Visualização

- **Dashboards Executivos** (Superset): Visão geral do sistema
- **Monitoramento Técnico** (Grafana): Métricas de infraestrutura
- **Análises Ad-hoc** (Jupyter): Notebooks interativos

---

## 🛠️ Tecnologias

### Core Stack

| Tecnologia | Versão | Função |
|-----------|--------|--------|
| **Python** | 3.10+ | Linguagem principal |
| **Apache Airflow** | 2.8.1 | Orquestração de pipelines |
| **Apache Spark** | 3.5.0 | Processamento distribuído |
| **PostgreSQL** | 15 | Database (Airflow + Serving) |
| **MinIO** | Latest | Data Lake (S3-compatible) |
| **Redis** | 7 | Cache e message broker |

### Monitoramento

| Tecnologia | Função |
|-----------|--------|
| **Prometheus** | Coleta de métricas |
| **Grafana** | Dashboards de monitoramento |
| **Apache Superset** | BI e dashboards analíticos |

### Bibliotecas Python

```
pandas, numpy, pyspark, psycopg2-binary, 
boto3, requests, prometheus-client, pytest
```

Veja [requirements.txt](requirements.txt) para lista completa.

---

## 🚀 Instalação

### Pré-requisitos

- Docker 20.10+
- Docker Compose 2.0+
- 8GB RAM mínimo (16GB recomendado)
- 20GB espaço em disco

### Quick Start

```bash
# 1. Clone o repositório
git clone https://github.com/rafarpl/sptrans-pipeline.git
cd sptrans-pipeline

# 2. Configure variáveis de ambiente
cp .env.example .env
# Edite .env e adicione seu token da API SPTrans

# 3. Execute setup inicial
chmod +x scripts/*.sh
./scripts/setup.sh

# 4. Inicie os serviços
make start
# ou: ./scripts/start_services.sh
```

### Configuração do Token da API

1. Obtenha seu token em: https://www.sptrans.com.br/desenvolvedores/
2. Adicione no arquivo `.env`:
   ```
   SPTRANS_API_TOKEN=seu_token_aqui
   ```

### Verificação

Aguarde ~2 minutos para todos os serviços iniciarem, então acesse:

- **Airflow**: http://localhost:8080 (admin/admin)
- **Spark UI**: http://localhost:8081
- **MinIO Console**: http://localhost:9001 (admin/miniopassword123)
- **Grafana**: http://localhost:3000 (admin/admin)
- **Superset**: http://localhost:8088 (admin/admin)

---

## 📖 Uso

### Comandos Make

```bash
# Gerenciamento de Serviços
make start          # Inicia todos os serviços
make stop           # Para todos os serviços
make restart        # Reinicia serviços
make ps             # Status dos containers
make logs           # Logs de todos os serviços

# Acesso Rápido (abre no navegador)
make airflow        # Abre Airflow UI
make spark          # Abre Spark UI
make minio          # Abre MinIO Console
make grafana        # Abre Grafana
make superset       # Abre Superset

# Database
make db-shell       # Acessa PostgreSQL
make db-create-tables  # Cria tabelas
make db-backup      # Backup do banco

# Data Lake
make minio-list-bronze  # Lista arquivos Bronze
make minio-list-silver  # Lista arquivos Silver
make minio-list-gold    # Lista arquivos Gold

# Testes
make test           # Executa todos os testes
make test-unit      # Testes unitários
make test-integration  # Testes de integração
make lint           # Linter (flake8)

# Desenvolvimento
make notebook       # Inicia Jupyter Lab
make shell          # Shell do container

# Informações
make help           # Lista todos os comandos
make status         # Status completo do sistema
make info           # Informações do projeto
```

### Executando DAGs

1. Acesse o Airflow: http://localhost:8080
2. Ative as DAGs desejadas:
   - `dag_01_gtfs_ingestion` - Ingestão diária GTFS
   - `dag_02_api_ingestion` - Ingestão API (2 min)
   - `dag_03_bronze_to_silver` - Transformação Silver
   - `dag_04_silver_to_gold` - Agregação Gold
   - `dag_05_gold_to_serving` - Load PostgreSQL

---

## 📁 Estrutura do Projeto

```
sptrans-pipeline/
├── config/                 # Configurações
│   ├── airflow/           # Airflow configs
│   ├── spark/             # Spark configs
│   ├── prometheus/        # Prometheus configs
│   └── grafana/           # Grafana datasources
│
├── dags/                   # Airflow DAGs
│   ├── dag_01_gtfs_ingestion.py
│   ├── dag_02_api_ingestion.py
│   ├── dag_03_bronze_to_silver.py
│   ├── dag_04_silver_to_gold.py
│   └── dag_05_gold_to_serving.py
│
├── docs/                   # Documentação
│   ├── 01_architecture.md
│   ├── 02_setup_guide.md
│   ├── 03_data_dictionary.md
│   └── ...
│
├── infra/                  # Infraestrutura
│   ├── docker/            # Dockerfiles
│   └── kubernetes/        # K8s manifests
│
├── notebooks/              # Jupyter notebooks
│   └── 01_exploratory_data_analysis.ipynb
│
├── scripts/                # Scripts utilitários
│   ├── setup.sh
│   ├── start_services.sh
│   └── backup_data.sh
│
├── sql/                    # Scripts SQL
│   ├── 01_serving_schema.sql
│   ├── 02_serving_tables.sql
│   ├── 03_materialized_views.sql
│   └── 04_indexes.sql
│
├── src/                    # Código fonte
│   ├── common/            # Código compartilhado
│   ├── ingestion/         # Camada de ingestão
│   ├── processing/        # Processamento Spark
│   ├── serving/           # Camada de serving
│   └── monitoring/        # Monitoramento
│
├── tests/                  # Testes
│   ├── unit/              # Testes unitários
│   ├── integration/       # Testes de integração
│   └── fixtures/          # Dados de teste
│
├── .env.example           # Template de variáveis
├── .gitignore             # Git ignore
├── docker-compose.yml     # Orquestração Docker
├── Makefile              # Comandos úteis
├── README.md             # Este arquivo
└── requirements.txt      # Dependências Python
```

---

## 🔄 DAGs do Airflow

### DAG 01 - GTFS Ingestion
- **Frequência**: Diária (2 AM)
- **Função**: Download e ingestão de dados GTFS estáticos
- **Output**: Bronze Layer (routes, stops, trips, shapes)

### DAG 02 - API Ingestion
- **Frequência**: A cada 2 minutos
- **Função**: Coleta de posições em tempo real
- **Output**: Bronze Layer (api_positions)

### DAG 03 - Bronze to Silver
- **Frequência**: A cada 30 minutos
- **Função**: Limpeza, validação e enriquecimento
- **Output**: Silver Layer (positions_enriched)

### DAG 04 - Silver to Gold
- **Frequência**: A cada hora
- **Função**: Agregações e cálculo de KPIs
- **Output**: Gold Layer (kpis_hourly, metrics_by_route, headway_analysis)

### DAG 05 - Gold to Serving
- **Frequência**: A cada hora (15 min após DAG 04)
- **Função**: Load para PostgreSQL
- **Output**: Serving Layer (PostgreSQL tables)

---

## 📊 Monitoramento

### Métricas Disponíveis

- **Pipeline**: Taxa de sucesso, tempo de execução, registros processados
- **Data Quality**: Completude, acurácia, validade
- **Infraestrutura**: CPU, memória, disco, rede
- **Aplicação**: Latência API, throughput Spark, queries PostgreSQL

### Dashboards

**Grafana** (http://localhost:3000):
- System Monitoring
- Data Quality
- Pipeline Performance

**Superset** (http://localhost:8088):
- Operational Dashboard
- Executive Summary
- Route Analysis

---

## 🧪 Testes

```bash
# Todos os testes
make test

# Testes unitários
make test-unit

# Testes de integração
make test-integration

# Com cobertura
make test-coverage

# Lint
make lint
```

### Cobertura de Testes

- Validadores: 95%+
- API Client: 90%+
- Transformações: 85%+
- Jobs Spark: 80%+

---

## 📚 Documentação Adicional

- [Arquitetura Detalhada](docs/01_architecture.md)
- [Guia de Instalação](docs/02_setup_guide.md)
- [Dicionário de Dados](docs/03_data_dictionary.md)
- [Referência da API](docs/04_api_reference.md)
- [Troubleshooting](docs/05_troubleshooting.md)
- [Justificativas Técnicas](docs/06_justifications.md)
- [Catálogo de Metadados](docs/07_metadata_catalog.md)

---

## 🤝 Contribuindo

Contribuições são bem-vindas! Por favor:

1. Fork o projeto
2. Crie uma branch para sua feature (`git checkout -b feature/AmazingFeature`)
3. Commit suas mudanças (`git commit -m 'Add some AmazingFeature'`)
4. Push para a branch (`git push origin feature/AmazingFeature`)
5. Abra um Pull Request

### Guidelines

- Siga o style guide PEP 8
- Adicione testes para novas features
- Atualize a documentação
- Mantenha cobertura de testes > 80%

---

## 📝 Licença

Este projeto está licenciado sob a Licença MIT - veja o arquivo [LICENSE](LICENSE) para detalhes.

---

## 👥 Autores

- **Rafael** - *Engenheiro de Dados* - [GitHub](https://github.com/rafarpl)

---

## 🙏 Agradecimentos

- **SPTrans** - Por disponibilizar a API Olho Vivo
- **Comunidade Open Source** - Pelas ferramentas incríveis

---

## 📞 Suporte

- 📧 Email: rafael@example.com
- 🐛 Issues: [GitHub Issues](https://github.com/rafarpl/sptrans-pipeline/issues)
- 📖 Docs: [Documentação Completa](docs/)

---

<div align="center">

**⭐ Se este projeto foi útil, considere dar uma estrela!**

Made with ❤️ and ☕ by Rafael

</div>

# 🚌 SPTrans Real-Time Pipeline

Pipeline de dados em tempo real para análise do sistema de transporte público de São Paulo.

**Projeto de Conclusão de Curso** - Engenharia de Dados | FIA/LABDATA

---

## 📊 Visão Geral

Pipeline completo de dados que processa **15.000 veículos** a cada **3 minutos**, gerando:
- 📈 **7,2 milhões de registros/dia**
- 🗺️ Posicionamento em tempo real
- 📊 KPIs e métricas de negócio
- 🎯 Dashboards interativos

---

## 🏗️ Arquitetura

### Medallion Architecture (Bronze → Silver → Gold)

```
┌─────────────┐
│  API GTFS   │ ──┐
└─────────────┘   │
                  ├──► ┌─────────┐    ┌─────────┐    ┌──────┐    ┌────────────┐
┌─────────────┐   │    │ BRONZE  │ ──►│ SILVER  │ ──►│ GOLD │ ──►│ PostgreSQL │
│ API Olho    │ ──┘    │ (MinIO) │    │ (Delta) │    │(Delta│    │  (Serving) │
│ Vivo (3min) │        └─────────┘    └─────────┘    └──────┘    └────────────┘
└─────────────┘              │              │            │               │
                             └──────────────┴────────────┴───────────────┘
                                           Airflow
```

### Stack Tecnológico

- **Storage**: MinIO (S3-compatible) + Delta Lake
- **Processing**: Apache Spark (1 Master + 2 Workers)
- **Orchestration**: Apache Airflow (7 DAGs)
- **Database**: PostgreSQL 15 + PostGIS
- **Monitoring**: Prometheus + Grafana
- **BI**: Apache Superset
- **Infra**: Docker Compose

---

## 🚀 Quick Start

### Pré-requisitos

- Docker & Docker Compose
- Python 3.9+
- 8GB RAM mínimo
- 50GB disco disponível

### Instalação

```bash
# 1. Clone o repositório
git clone https://github.com/rafarpl/sp-trans-pipeline.git
cd sp-trans-pipeline

# 2. Configure variáveis de ambiente
cp config/.env.example config/.env
# Edite .env com suas credenciais da API SPTrans

# 3. Inicialize a infraestrutura
make setup
make up

# 4. Crie o database
make db-init

# 5. Execute o pipeline
make airflow-trigger-all
```

### Acesso aos Serviços

| Serviço | URL | Credenciais |
|---------|-----|-------------|
| **Airflow** | http://localhost:8080 | admin / admin |
| **Superset** | http://localhost:8088 | admin / admin |
| **Grafana** | http://localhost:3000 | admin / admin |
| **MinIO** | http://localhost:9001 | minioadmin / minioadmin |

---

## 📁 Estrutura do Projeto

```
sp-trans-pipeline/
├── config/                 # Configurações
│   ├── .env.example
│   ├── spark/
│   ├── prometheus/
│   └── grafana/
├── dags/                   # DAGs Airflow (7)
├── sql/                    # Scripts SQL (8)
├── src/
│   ├── common/            # Módulos base
│   ├── ingestion/         # Ingestão de dados
│   ├── processing/        # Jobs Spark
│   ├── serving/           # Serving layer
│   └── monitoring/        # Monitoramento
├── tests/                 # Testes
│   ├── unit/
│   └── integration/
├── docs/                  # Documentação
├── docker-compose.yml
├── Makefile
└── requirements.txt
```

---

## 🔄 Pipeline Flow

### 1. Ingestão (Bronze)
- **API Olho Vivo**: A cada 3 minutos
- **GTFS Estático**: Diariamente às 02:00
- **Formato**: Parquet particionado

### 2. Transformação (Silver)
- **Limpeza**: Remoção de nulos, outliers
- **Normalização**: Estrutura tabular
- **Enriquecimento**: Geocoding, telemetria
- **Formato**: Delta Lake

### 3. Agregação (Gold)
- **Métricas Horárias**: Por linha
- **Sumarização Diária**: Sistema completo
- **KPIs**: Velocidade, headway, congestão
- **Formato**: Delta Lake

### 4. Serving (PostgreSQL)
- **Load**: A cada 10 minutos
- **Otimização**: Índices, MVs, partições
- **Consumo**: Dashboards, APIs

---

## 📊 KPIs Calculados

### Operacionais
- 🚌 Frota ativa por linha
- ⏱️ Headway (intervalo entre veículos)
- 🎯 Taxa de pontualidade
- 📈 Cobertura de serviço

### Performance
- ⚡ Velocidade média
- 🚦 Índice de congestionamento
- 📏 Distância percorrida
- 🔄 Reliability score

### Qualidade
- ✅ Data quality score
- 🔍 Completude de dados
- 🎲 Taxa de duplicatas
- ⏰ Freshness

---

## 🧪 Testes

```bash
# Todos os testes
make test

# Unit tests
make test-unit

# Integration tests
make test-integration

# Com coverage
make test-coverage
```

---

## 📚 Documentação

Documentação completa em `/docs`:
- [Arquitetura](docs/01_architecture.md)
- [Setup Guide](docs/02_setup_guide.md)
- [Data Dictionary](docs/03_data_dictionary.md)
- [API Reference](docs/04_api_reference.md)
- [Troubleshooting](docs/05_troubleshooting.md)

---

## 🔧 Comandos Úteis (Makefile)

```bash
# Setup inicial
make setup                 # Configuração completa
make up                    # Iniciar todos serviços
make down                  # Parar todos serviços

# Database
make db-init              # Criar database
make db-migrate           # Executar migrations
make db-backup            # Backup completo

# Airflow
make airflow-init         # Inicializar Airflow
make airflow-trigger-all  # Trigger todos DAGs

# Spark
make spark-submit         # Submit job Spark
make spark-shell          # Spark shell interativo

# MinIO
make minio-create-buckets # Criar buckets
make minio-list          # Listar objetos

# Monitoramento
make logs                # Ver logs de todos serviços
make status              # Status dos serviços

# Limpeza
make clean               # Limpar dados temporários
make clean-all           # Reset completo
```

---

## 📈 Métricas & Monitoramento

### Prometheus Metrics
- `sptrans_pipeline_records_processed_total`
- `sptrans_pipeline_duration_seconds`
- `sptrans_pipeline_data_quality_score`
- `sptrans_pipeline_api_requests_total`

### Grafana Dashboards
1. **System Overview**: CPU, memória, disco
2. **Pipeline Performance**: Duração, throughput
3. **Data Quality**: Scores, validações
4. **Business KPIs**: Frota, velocidade, headway

---

## 🎓 Conformidade Acadêmica

### Requisitos Atendidos ✅
- ✅ Near real-time (< 3 minutos)
- ✅ Data Lake com múltiplas camadas
- ✅ GTFS integrado
- ✅ Enriquecimento (geocoding)
- ✅ KPIs e métricas
- ✅ Dashboards interativos
- ✅ 100% Open Source
- ✅ Documentação completa
- ✅ Código fonte versionado

---

## 👨‍💻 Autor

**Rafael** (rafarpl)  
Pós-Graduação em Engenharia de Dados  
FIA/LABDATA - 2024

---

## 📝 Licença

MIT License - Veja [LICENSE](LICENSE) para detalhes

---

## 🙏 Agradecimentos

- **SPTrans** pelos dados públicos
- **FIA/LABDATA** pela orientação
- **Professores** pelo feedback
- **Comunidade Open Source**

---

## 📞 Suporte

- 🐛 **Issues**: [GitHub Issues](https://github.com/rafarpl/sp-trans-pipeline/issues)
- 📧 **Email**: contato@exemplo.com
- 💬 **Discussions**: [GitHub Discussions](https://github.com/rafarpl/sp-trans-pipeline/discussions)

---

**⭐ Se este projeto foi útil, considere dar uma estrela no GitHub!**

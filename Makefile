# ============================================================================
# SPTRANS PIPELINE - MAKEFILE
# ============================================================================
# Comandos úteis para gerenciamento do projeto
# 
# Uso: make <comando>
# Exemplo: make start
# ============================================================================

.PHONY: help install start stop restart logs clean test backup restore build deploy

# Variáveis
PROJECT_NAME = sptrans-pipeline
DOCKER_COMPOSE = docker-compose
PYTHON = python3
PIP = pip3

# Cores para output
RED = \033[0;31m
GREEN = \033[0;32m
YELLOW = \033[1;33m
BLUE = \033[0;34m
NC = \033[0m # No Color

# ============================================================================
# HELP
# ============================================================================

help: ## Mostra esta mensagem de ajuda
	@echo ""
	@echo "$(BLUE)╔══════════════════════════════════════════════════════════════╗$(NC)"
	@echo "$(BLUE)║                                                              ║$(NC)"
	@echo "$(BLUE)║         SPTRANS PIPELINE - COMANDOS DISPONÍVEIS              ║$(NC)"
	@echo "$(BLUE)║                                                              ║$(NC)"
	@echo "$(BLUE)╚══════════════════════════════════════════════════════════════╝$(NC)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "$(GREEN)%-20s$(NC) %s\n", $$1, $$2}'
	@echo ""

# ============================================================================
# SETUP E INSTALAÇÃO
# ============================================================================

install: ## Instala todas as dependências
	@echo "$(BLUE)📦 Instalando dependências...$(NC)"
	$(PIP) install -r requirements.txt
	@echo "$(GREEN)✅ Dependências instaladas com sucesso!$(NC)"

setup: ## Executa setup inicial completo do projeto
	@echo "$(BLUE)🚀 Executando setup inicial...$(NC)"
	chmod +x scripts/*.sh
	./scripts/setup.sh
	@echo "$(GREEN)✅ Setup concluído!$(NC)"

env: ## Cria arquivo .env a partir do .env.example
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "$(GREEN)✅ Arquivo .env criado! Configure suas variáveis.$(NC)"; \
	else \
		echo "$(YELLOW)⚠️  Arquivo .env já existe$(NC)"; \
	fi

# ============================================================================
# DOCKER & SERVICES
# ============================================================================

build: ## Build de todas as imagens Docker
	@echo "$(BLUE)🏗️  Building Docker images...$(NC)"
	$(DOCKER_COMPOSE) build
	@echo "$(GREEN)✅ Images criadas com sucesso!$(NC)"

start: ## Inicia todos os serviços
	@echo "$(BLUE)🚀 Iniciando serviços...$(NC)"
	./scripts/start_services.sh

stop: ## Para todos os serviços
	@echo "$(YELLOW)🛑 Parando serviços...$(NC)"
	./scripts/stop_services.sh

restart: stop start ## Reinicia todos os serviços
	@echo "$(GREEN)✅ Serviços reiniciados!$(NC)"

ps: ## Lista status dos containers
	@echo "$(BLUE)📊 Status dos containers:$(NC)"
	$(DOCKER_COMPOSE) ps

logs: ## Mostra logs de todos os serviços
	$(DOCKER_COMPOSE) logs -f

logs-airflow: ## Mostra logs do Airflow
	$(DOCKER_COMPOSE) logs -f airflow-webserver airflow-scheduler

logs-spark: ## Mostra logs do Spark
	$(DOCKER_COMPOSE) logs -f spark-master spark-worker-1 spark-worker-2

logs-postgres: ## Mostra logs do PostgreSQL
	$(DOCKER_COMPOSE) logs -f postgres

# ============================================================================
# ACESSO AOS SERVIÇOS
# ============================================================================

airflow: ## Abre Airflow no navegador
	@echo "$(BLUE)🌐 Abrindo Airflow...$(NC)"
	@echo "$(GREEN)http://localhost:8080$(NC)"
	@echo "User: admin | Password: admin"
	@python3 -m webbrowser http://localhost:8080 2>/dev/null || \
	xdg-open http://localhost:8080 2>/dev/null || \
	open http://localhost:8080 2>/dev/null || \
	echo "Abra manualmente: http://localhost:8080"

spark: ## Abre Spark UI no navegador
	@echo "$(BLUE)🌐 Abrindo Spark UI...$(NC)"
	@echo "$(GREEN)http://localhost:8081$(NC)"
	@python3 -m webbrowser http://localhost:8081 2>/dev/null || \
	xdg-open http://localhost:8081 2>/dev/null || \
	open http://localhost:8081 2>/dev/null || \
	echo "Abra manualmente: http://localhost:8081"

minio: ## Abre MinIO console no navegador
	@echo "$(BLUE)🌐 Abrindo MinIO Console...$(NC)"
	@echo "$(GREEN)http://localhost:9001$(NC)"
	@echo "User: admin | Password: miniopassword123"
	@python3 -m webbrowser http://localhost:9001 2>/dev/null || \
	xdg-open http://localhost:9001 2>/dev/null || \
	open http://localhost:9001 2>/dev/null || \
	echo "Abra manualmente: http://localhost:9001"

grafana: ## Abre Grafana no navegador
	@echo "$(BLUE)🌐 Abrindo Grafana...$(NC)"
	@echo "$(GREEN)http://localhost:3000$(NC)"
	@echo "User: admin | Password: admin"
	@python3 -m webbrowser http://localhost:3000 2>/dev/null || \
	xdg-open http://localhost:3000 2>/dev/null || \
	open http://localhost:3000 2>/dev/null || \
	echo "Abra manualmente: http://localhost:3000"

superset: ## Abre Superset no navegador
	@echo "$(BLUE)🌐 Abrindo Superset...$(NC)"
	@echo "$(GREEN)http://localhost:8088$(NC)"
	@echo "User: admin | Password: admin"
	@python3 -m webbrowser http://localhost:8088 2>/dev/null || \
	xdg-open http://localhost:8088 2>/dev/null || \
	open http://localhost:8088 2>/dev/null || \
	echo "Abra manualmente: http://localhost:8088"

# ============================================================================
# DATABASE
# ============================================================================

db-shell: ## Acessa shell do PostgreSQL
	@echo "$(BLUE)🗄️  Acessando PostgreSQL...$(NC)"
	$(DOCKER_COMPOSE) exec postgres psql -U airflow -d airflow

db-create-tables: ## Cria tabelas da serving layer
	@echo "$(BLUE)🗄️  Criando tabelas...$(NC)"
	$(DOCKER_COMPOSE) exec -T postgres psql -U airflow -d airflow < sql/01_serving_schema.sql
	$(DOCKER_COMPOSE) exec -T postgres psql -U airflow -d airflow < sql/02_serving_tables.sql
	@echo "$(GREEN)✅ Tabelas criadas!$(NC)"

db-create-views: ## Cria views materializadas
	@echo "$(BLUE)🗄️  Criando views...$(NC)"
	$(DOCKER_COMPOSE) exec -T postgres psql -U airflow -d airflow < sql/03_materialized_views.sql
	@echo "$(GREEN)✅ Views criadas!$(NC)"

db-backup: ## Faz backup do banco de dados
	@echo "$(BLUE)💾 Fazendo backup...$(NC)"
	./scripts/backup_data.sh
	@echo "$(GREEN)✅ Backup concluído!$(NC)"

# ============================================================================
# DATALAKE
# ============================================================================

minio-shell: ## Acessa MinIO Client (mc)
	@echo "$(BLUE)📦 Acessando MinIO Client...$(NC)"
	$(DOCKER_COMPOSE) exec minio sh

minio-list-bronze: ## Lista arquivos na camada Bronze
	@echo "$(BLUE)📊 Listando Bronze Layer...$(NC)"
	$(DOCKER_COMPOSE) exec minio mc ls local/sptrans-datalake/bronze/ --recursive

minio-list-silver: ## Lista arquivos na camada Silver
	@echo "$(BLUE)📊 Listando Silver Layer...$(NC)"
	$(DOCKER_COMPOSE) exec minio mc ls local/sptrans-datalake/silver/ --recursive

minio-list-gold: ## Lista arquivos na camada Gold
	@echo "$(BLUE)📊 Listando Gold Layer...$(NC)"
	$(DOCKER_COMPOSE) exec minio mc ls local/sptrans-datalake/gold/ --recursive

# ============================================================================
# TESTES
# ============================================================================

test: ## Executa todos os testes
	@echo "$(BLUE)🧪 Executando testes...$(NC)"
	$(PYTHON) -m pytest tests/ -v --cov=src --cov-report=html
	@echo "$(GREEN)✅ Testes concluídos!$(NC)"

test-unit: ## Executa testes unitários
	@echo "$(BLUE)🧪 Executando testes unitários...$(NC)"
	$(PYTHON) -m pytest tests/unit/ -v

test-integration: ## Executa testes de integração
	@echo "$(BLUE)🧪 Executando testes de integração...$(NC)"
	$(PYTHON) -m pytest tests/integration/ -v

test-coverage: ## Gera relatório de cobertura
	@echo "$(BLUE)📊 Gerando relatório de cobertura...$(NC)"
	$(PYTHON) -m pytest tests/ --cov=src --cov-report=html --cov-report=term
	@echo "$(GREEN)✅ Relatório disponível em htmlcov/index.html$(NC)"

lint: ## Executa linter (flake8)
	@echo "$(BLUE)🔍 Executando linter...$(NC)"
	$(PYTHON) -m flake8 src/ tests/ dags/
	@echo "$(GREEN)✅ Linting concluído!$(NC)"

format: ## Formata código com black
	@echo "$(BLUE)✨ Formatando código...$(NC)"
	$(PYTHON) -m black src/ tests/ dags/
	@echo "$(GREEN)✅ Código formatado!$(NC)"

# ============================================================================
# JUPYTER
# ============================================================================

notebook: ## Inicia Jupyter Lab
	@echo "$(BLUE)📓 Iniciando Jupyter Lab...$(NC)"
	$(PYTHON) -m jupyter lab notebooks/

# ============================================================================
# DADOS E BACKUP
# ============================================================================

backup: ## Backup completo (DB + Data Lake)
	@echo "$(BLUE)💾 Iniciando backup completo...$(NC)"
	./scripts/backup_data.sh --full
	@echo "$(GREEN)✅ Backup completo concluído!$(NC)"

backup-incremental: ## Backup incremental (últimas 24h)
	@echo "$(BLUE)💾 Iniciando backup incremental...$(NC)"
	./scripts/backup_data.sh --incremental
	@echo "$(GREEN)✅ Backup incremental concluído!$(NC)"

restore: ## Restaura backup (uso: make restore BACKUP=nome_do_backup)
	@if [ -z "$(BACKUP)" ]; then \
		echo "$(RED)❌ Erro: Especifique o backup$(NC)"; \
		echo "Uso: make restore BACKUP=sptrans_backup_20250101_120000"; \
		exit 1; \
	fi
	@echo "$(YELLOW)⚠️  Restaurando backup: $(BACKUP)$(NC)"
	./scripts/restore_data.sh $(BACKUP)

# ============================================================================
# LIMPEZA
# ============================================================================

clean: ## Remove arquivos temporários e cache
	@echo "$(YELLOW)🧹 Limpando arquivos temporários...$(NC)"
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.log" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".coverage" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "htmlcov" -exec rm -rf {} + 2>/dev/null || true
	@echo "$(GREEN)✅ Limpeza concluída!$(NC)"

clean-all: clean ## Remove tudo (containers, volumes, imagens)
	@echo "$(RED)⚠️  ATENÇÃO: Isso vai remover TODOS os dados!$(NC)"
	@read -p "Tem certeza? (y/N): " confirm && [ "$$confirm" = "y" ] || exit 1
	@echo "$(YELLOW)🧹 Removendo containers, volumes e imagens...$(NC)"
	$(DOCKER_COMPOSE) down -v --remove-orphans
	docker system prune -af --volumes
	@echo "$(GREEN)✅ Tudo removido!$(NC)"

# ============================================================================
# MONITORAMENTO
# ============================================================================

status: ## Mostra status completo do sistema
	@echo ""
	@echo "$(BLUE)╔══════════════════════════════════════════════════════════════╗$(NC)"
	@echo "$(BLUE)║                 STATUS DO SISTEMA                            ║$(NC)"
	@echo "$(BLUE)╚══════════════════════════════════════════════════════════════╝$(NC)"
	@echo ""
	@echo "$(YELLOW)🐳 Docker Containers:$(NC)"
	@$(DOCKER_COMPOSE) ps
	@echo ""
	@echo "$(YELLOW)💾 Uso de Disco:$(NC)"
	@df -h | grep -E "Filesystem|/var/lib/docker|sptrans" || echo "N/A"
	@echo ""
	@echo "$(YELLOW)🌐 URLs de Acesso:$(NC)"
	@echo "  • Airflow:   $(GREEN)http://localhost:8080$(NC)"
	@echo "  • Spark UI:  $(GREEN)http://localhost:8081$(NC)"
	@echo "  • MinIO:     $(GREEN)http://localhost:9001$(NC)"
	@echo "  • Grafana:   $(GREEN)http://localhost:3000$(NC)"
	@echo "  • Superset:  $(GREEN)http://localhost:8088$(NC)"
	@echo ""

health: ## Verifica saúde dos serviços
	@echo "$(BLUE)🏥 Verificando saúde dos serviços...$(NC)"
	@echo ""
	@echo "$(YELLOW)Airflow:$(NC)"
	@curl -s http://localhost:8080/health | grep -q "healthy" && echo "  $(GREEN)✅ OK$(NC)" || echo "  $(RED)❌ Falha$(NC)"
	@echo "$(YELLOW)MinIO:$(NC)"
	@curl -s http://localhost:9000/minio/health/live | grep -q "OK" && echo "  $(GREEN)✅ OK$(NC)" || echo "  $(RED)❌ Falha$(NC)"
	@echo ""

metrics: ## Mostra métricas Prometheus
	@echo "$(BLUE)📊 Métricas (últimas 5 minutos):$(NC)"
	@curl -s http://localhost:9090/api/v1/query?query=up | jq . 2>/dev/null || echo "Prometheus não disponível"

# ============================================================================
# DESENVOLVIMENTO
# ============================================================================

dev: ## Modo desenvolvimento (com hot reload)
	@echo "$(BLUE)🔧 Iniciando modo desenvolvimento...$(NC)"
	$(DOCKER_COMPOSE) -f docker-compose.yml -f docker-compose.dev.yml up

shell: ## Acessa shell do container principal
	@echo "$(BLUE)🐚 Acessando shell...$(NC)"
	$(DOCKER_COMPOSE) exec airflow-webserver bash

spark-shell: ## Acessa Spark shell
	@echo "$(BLUE)⚡ Acessando Spark shell...$(NC)"
	$(DOCKER_COMPOSE) exec spark-master spark-shell

python-shell: ## Acessa Python shell com imports do projeto
	@echo "$(BLUE)🐍 Acessando Python shell...$(NC)"
	$(PYTHON) -i -c "import sys; sys.path.insert(0, 'src'); print('Imports: from src.common import *')"

# ============================================================================
# CI/CD
# ============================================================================

ci: lint test ## Executa pipeline de CI (lint + tests)
	@echo "$(GREEN)✅ Pipeline CI concluído com sucesso!$(NC)"

deploy-prod: ## Deploy para produção
	@echo "$(YELLOW)🚀 Deploying to production...$(NC)"
	@echo "$(RED)⚠️  Funcionalidade ainda não implementada$(NC)"

# ============================================================================
# DOCUMENTAÇÃO
# ============================================================================

docs: ## Gera documentação do projeto
	@echo "$(BLUE)📚 Gerando documentação...$(NC)"
	cd docs && make html
	@echo "$(GREEN)✅ Documentação gerada em docs/_build/html/$(NC)"

docs-serve: ## Serve documentação localmente
	@echo "$(BLUE)🌐 Servindo documentação...$(NC)"
	cd docs/_build/html && $(PYTHON) -m http.server 8000

# ============================================================================
# INFORMAÇÕES
# ============================================================================

info: ## Mostra informações do projeto
	@echo ""
	@echo "$(BLUE)╔══════════════════════════════════════════════════════════════╗$(NC)"
	@echo "$(BLUE)║                                                              ║$(NC)"
	@echo "$(BLUE)║              SPTRANS PIPELINE - INFORMAÇÕES                  ║$(NC)"
	@echo "$(BLUE)║                                                              ║$(NC)"
	@echo "$(BLUE)╚══════════════════════════════════════════════════════════════╝$(NC)"
	@echo ""
	@echo "$(YELLOW)📦 Projeto:$(NC)       SPTrans Data Pipeline"
	@echo "$(YELLOW)🎯 Objetivo:$(NC)      Pipeline de dados para transporte público de SP"
	@echo "$(YELLOW)🏗️  Arquitetura:$(NC)  Lakehouse com camadas Bronze/Silver/Gold"
	@echo "$(YELLOW)🔧 Stack:$(NC)         Airflow, Spark, PostgreSQL, MinIO, Superset"
	@echo "$(YELLOW)📊 Camadas:$(NC)       Bronze → Silver → Gold → Serving"
	@echo "$(YELLOW)⚡ Execução:$(NC)      API: 2min | Transformação: 1h | Agregação: 1h"
	@echo ""
	@echo "$(YELLOW)📖 Documentação:$(NC)  docs/README.md"
	@echo "$(YELLOW)🐛 Issues:$(NC)        GitHub Issues"
	@echo "$(YELLOW)💬 Contato:$(NC)       [seu-email]"
	@echo ""

version: ## Mostra versões dos componentes
	@echo "$(BLUE)📋 Versões dos Componentes:$(NC)"
	@echo ""
	@echo "$(YELLOW)Python:$(NC)        $$($(PYTHON) --version)"
	@echo "$(YELLOW)Docker:$(NC)        $$(docker --version)"
	@echo "$(YELLOW)Docker Compose:$(NC) $$($(DOCKER_COMPOSE) --version)"
	@echo ""

# ============================================================================
# DEFAULT
# ============================================================================

.DEFAULT_GOAL := help

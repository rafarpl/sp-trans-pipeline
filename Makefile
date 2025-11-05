.PHONY: help setup up down restart logs status clean test install-deps

# ==================================
# COLORS
# ==================================
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m  # No Color

# ==================================
# HELP
# ==================================
help:  ## Show this help message
	@echo "$(BLUE)SPTrans Real-Time Pipeline - Makefile Commands$(NC)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'
	@echo ""

# ==================================
# SETUP
# ==================================
setup:  ## Initial setup (create .env, install dependencies)
	@echo "$(BLUE)Setting up SPTrans Pipeline...$(NC)"
	@if [ ! -f .env ]; then \
		cp config/.env.example .env; \
		echo "$(GREEN)✓ Created .env file$(NC)"; \
	else \
		echo "$(YELLOW)⚠ .env file already exists$(NC)"; \
	fi
	@mkdir -p data/gtfs logs/airflow logs/spark data/backups
	@echo "$(GREEN)✓ Created directory structure$(NC)"
	@echo "$(YELLOW)⚠ Don't forget to add your SPTRANS_API_TOKEN in .env$(NC)"

install-deps:  ## Install Python dependencies
	@echo "$(BLUE)Installing dependencies...$(NC)"
	pip install -r requirements.txt
	@echo "$(GREEN)✓ Dependencies installed$(NC)"

# ==================================
# DOCKER COMPOSE
# ==================================
up:  ## Start all services
	@echo "$(BLUE)Starting all services...$(NC)"
	docker-compose up -d
	@echo "$(GREEN)✓ All services started$(NC)"
	@make urls

down:  ## Stop all services
	@echo "$(BLUE)Stopping all services...$(NC)"
	docker-compose down
	@echo "$(GREEN)✓ All services stopped$(NC)"

restart:  ## Restart all services
	@echo "$(BLUE)Restarting all services...$(NC)"
	docker-compose restart
	@echo "$(GREEN)✓ All services restarted$(NC)"

stop:  ## Stop all services (alias for down)
	@make down

# ==================================
# INDIVIDUAL SERVICES
# ==================================
up-postgres:  ## Start PostgreSQL only
	docker-compose up -d postgres

up-minio:  ## Start MinIO only
	docker-compose up -d minio minio-client

up-spark:  ## Start Spark cluster only
	docker-compose up -d spark-master spark-worker-1 spark-worker-2

up-airflow:  ## Start Airflow services only
	docker-compose up -d airflow-webserver airflow-scheduler airflow-worker

up-superset:  ## Start Superset only
	docker-compose up -d superset

up-monitoring:  ## Start Prometheus + Grafana only
	docker-compose up -d prometheus grafana

# ==================================
# LOGS
# ==================================
logs:  ## Show logs from all services
	docker-compose logs -f

logs-airflow:  ## Show Airflow logs only
	docker-compose logs -f airflow-webserver airflow-scheduler airflow-worker

logs-spark:  ## Show Spark logs only
	docker-compose logs -f spark-master spark-worker-1 spark-worker-2

logs-postgres:  ## Show PostgreSQL logs only
	docker-compose logs -f postgres

logs-minio:  ## Show MinIO logs only
	docker-compose logs -f minio

# ==================================
# STATUS & HEALTH
# ==================================
status:  ## Show status of all services
	@echo "$(BLUE)Service Status:$(NC)"
	@docker-compose ps

health:  ## Check health of all services
	@echo "$(BLUE)Checking service health...$(NC)"
	@docker-compose ps | grep -E "Up|healthy" && echo "$(GREEN)✓ Services healthy$(NC)" || echo "$(RED)✗ Some services unhealthy$(NC)"

urls:  ## Show all service URLs
	@echo ""
	@echo "$(BLUE)═══════════════════════════════════════════════════$(NC)"
	@echo "$(GREEN)  SPTrans Pipeline - Service URLs$(NC)"
	@echo "$(BLUE)═══════════════════════════════════════════════════$(NC)"
	@echo ""
	@echo "$(YELLOW)Orchestration:$(NC)"
	@echo "  • Airflow:      $(GREEN)http://localhost:8080$(NC)  (admin/admin)"
	@echo ""
	@echo "$(YELLOW)Storage:$(NC)"
	@echo "  • MinIO Console: $(GREEN)http://localhost:9001$(NC)  (minioadmin/minioadmin123)"
	@echo "  • MinIO API:     $(GREEN)http://localhost:9000$(NC)"
	@echo ""
	@echo "$(YELLOW)Processing:$(NC)"
	@echo "  • Spark Master:  $(GREEN)http://localhost:8081$(NC)"
	@echo ""
	@echo "$(YELLOW)Visualization:$(NC)"
	@echo "  • Superset:      $(GREEN)http://localhost:8088$(NC)  (admin/admin)"
	@echo ""
	@echo "$(YELLOW)Monitoring:$(NC)"
	@echo "  • Grafana:       $(GREEN)http://localhost:3000$(NC)  (admin/admin)"
	@echo "  • Prometheus:    $(GREEN)http://localhost:9090$(NC)"
	@echo ""
	@echo "$(YELLOW)Database:$(NC)"
	@echo "  • PostgreSQL:    $(GREEN)localhost:5432$(NC)  (sptrans/sptrans123)"
	@echo "  • Redis:         $(GREEN)localhost:6379$(NC)"
	@echo ""
	@echo "$(BLUE)═══════════════════════════════════════════════════$(NC)"
	@echo ""

# ==================================
# DATABASE
# ==================================
db-init:  ## Initialize database schema
	@echo "$(BLUE)Initializing database...$(NC)"
	docker-compose exec postgres psql -U sptrans -d sptrans -f /docker-entrypoint-initdb.d/00_create_databases.sql
	@echo "$(GREEN)✓ Database initialized$(NC)"

db-shell:  ## Open PostgreSQL shell
	docker-compose exec postgres psql -U sptrans -d sptrans

db-backup:  ## Backup PostgreSQL database
	@echo "$(BLUE)Backing up database...$(NC)"
	@mkdir -p data/backups
	docker-compose exec postgres pg_dump -U sptrans sptrans > data/backups/backup_$$(date +%Y%m%d_%H%M%S).sql
	@echo "$(GREEN)✓ Backup created$(NC)"

db-restore:  ## Restore PostgreSQL database (set BACKUP_FILE=path/to/backup.sql)
	@if [ -z "$(BACKUP_FILE)" ]; then \
		echo "$(RED)✗ Error: BACKUP_FILE not set$(NC)"; \
		echo "Usage: make db-restore BACKUP_FILE=data/backups/backup_20251104.sql"; \
		exit 1; \
	fi
	@echo "$(BLUE)Restoring database from $(BACKUP_FILE)...$(NC)"
	docker-compose exec -T postgres psql -U sptrans -d sptrans < $(BACKUP_FILE)
	@echo "$(GREEN)✓ Database restored$(NC)"

# ==================================
# MINIO / DATA LAKE
# ==================================
minio-shell:  ## Open MinIO client shell
	docker-compose exec minio-client mc alias set minio http://minio:9000 minioadmin minioadmin123

minio-list:  ## List all buckets and objects
	docker-compose exec minio-client mc ls minio/

minio-create-buckets:  ## Create required buckets
	docker-compose exec minio-client sh -c " \
		mc alias set minio http://minio:9000 minioadmin minioadmin123 && \
		mc mb minio/sptrans-datalake --ignore-existing && \
		mc mb minio/spark-logs --ignore-existing && \
		echo 'Buckets created' \
	"

# ==================================
# SPARK
# ==================================
spark-shell:  ## Open Spark shell (PySpark)
	docker-compose exec spark-master spark-shell

pyspark:  ## Open PySpark shell
	docker-compose exec spark-master pyspark

spark-submit:  ## Submit Spark job (set JOB_FILE=path/to/job.py)
	@if [ -z "$(JOB_FILE)" ]; then \
		echo "$(RED)✗ Error: JOB_FILE not set$(NC)"; \
		echo "Usage: make spark-submit JOB_FILE=src/processing/jobs/ingest_api_to_bronze.py"; \
		exit 1; \
	fi
	docker-compose exec spark-master spark-submit --master spark://spark-master:7077 /opt/spark-apps/$(JOB_FILE)

# ==================================
# AIRFLOW
# ==================================
airflow-shell:  ## Open Airflow CLI
	docker-compose exec airflow-webserver bash

airflow-dags-list:  ## List all DAGs
	docker-compose exec airflow-webserver airflow dags list

airflow-dags-trigger:  ## Trigger a DAG (set DAG_ID=dag_name)
	@if [ -z "$(DAG_ID)" ]; then \
		echo "$(RED)✗ Error: DAG_ID not set$(NC)"; \
		echo "Usage: make airflow-dags-trigger DAG_ID=dag_02_api_ingestion"; \
		exit 1; \
	fi
	docker-compose exec airflow-webserver airflow dags trigger $(DAG_ID)

airflow-create-user:  ## Create Airflow admin user
	docker-compose exec airflow-webserver airflow users create \
		--username admin \
		--firstname Admin \
		--lastname User \
		--role Admin \
		--email admin@sptrans.com \
		--password admin

# ==================================
# TESTING
# ==================================
test:  ## Run all tests
	@echo "$(BLUE)Running tests...$(NC)"
	pytest tests/ -v --cov=src --cov-report=html --cov-report=term
	@echo "$(GREEN)✓ Tests completed$(NC)"

test-unit:  ## Run unit tests only
	pytest tests/unit/ -v

test-integration:  ## Run integration tests only
	pytest tests/integration/ -v

test-coverage:  ## Generate coverage report
	pytest tests/ --cov=src --cov-report=html
	@echo "$(GREEN)✓ Coverage report: htmlcov/index.html$(NC)"

# ==================================
# CODE QUALITY
# ==================================
lint:  ## Run code linters
	@echo "$(BLUE)Running linters...$(NC)"
	black src/ --check
	flake8 src/
	mypy src/
	@echo "$(GREEN)✓ Linting completed$(NC)"

format:  ## Format code with black
	@echo "$(BLUE)Formatting code...$(NC)"
	black src/ dags/ tests/
	@echo "$(GREEN)✓ Code formatted$(NC)"

# ==================================
# DEVELOPMENT
# ==================================
dev-setup:  ## Setup development environment
	@make setup
	@make install-deps
	pre-commit install
	@echo "$(GREEN)✓ Development environment ready$(NC)"

dev-run:  ## Run in development mode (with hot reload)
	docker-compose -f docker-compose.yml -f docker-compose.dev.yml up

# ==================================
# DATA INGESTION
# ==================================
ingest-gtfs:  ## Download and ingest GTFS data
	@echo "$(BLUE)Ingesting GTFS data...$(NC)"
	python src/ingestion/gtfs_downloader.py
	@echo "$(GREEN)✓ GTFS data ingested$(NC)"

ingest-api:  ## Test API ingestion
	@echo "$(BLUE)Testing API ingestion...$(NC)"
	python src/ingestion/sptrans_api_client.py
	@echo "$(GREEN)✓ API test completed$(NC)"

# ==================================
# MONITORING
# ==================================
monitor-logs:  ## Monitor logs in real-time (all services)
	docker-compose logs -f --tail=100

monitor-metrics:  ## Open Prometheus in browser
	@echo "$(BLUE)Opening Prometheus...$(NC)"
	@python -m webbrowser http://localhost:9090

monitor-dashboard:  ## Open Grafana in browser
	@echo "$(BLUE)Opening Grafana...$(NC)"
	@python -m webbrowser http://localhost:3000

# ==================================
# CLEANUP
# ==================================
clean:  ## Clean up containers and volumes (DESTRUCTIVE!)
	@echo "$(RED)⚠ WARNING: This will remove all containers and volumes!$(NC)"
	@echo "Press Ctrl+C to cancel or wait 5 seconds..."
	@sleep 5
	docker-compose down -v
	@echo "$(GREEN)✓ Cleanup completed$(NC)"

clean-logs:  ## Clean log files
	@echo "$(BLUE)Cleaning logs...$(NC)"
	rm -rf logs/airflow/* logs/spark/*
	@echo "$(GREEN)✓ Logs cleaned$(NC)"

clean-data:  ## Clean temporary data (DESTRUCTIVE!)
	@echo "$(RED)⚠ WARNING: This will remove all temporary data!$(NC)"
	@echo "Press Ctrl+C to cancel or wait 5 seconds..."
	@sleep 5
	rm -rf data/gtfs/* logs/* data/backups/*
	@echo "$(GREEN)✓ Data cleaned$(NC)"

prune:  ## Prune Docker system (images, containers, networks)
	@echo "$(BLUE)Pruning Docker system...$(NC)"
	docker system prune -af --volumes
	@echo "$(GREEN)✓ Docker pruned$(NC)"

# ==================================
# DOCUMENTATION
# ==================================
docs:  ## Generate documentation
	@echo "$(BLUE)Generating documentation...$(NC)"
	cd docs && make html
	@echo "$(GREEN)✓ Documentation generated$(NC)"

docs-serve:  ## Serve documentation locally
	cd docs/_build/html && python -m http.server 8000

# ==================================
# PRESENTATION
# ==================================
presentation:  ## Generate final presentation
	@echo "$(BLUE)Generating presentation...$(NC)"
	# Add presentation generation command here
	@echo "$(GREEN)✓ Presentation generated$(NC)"

# ==================================
# DEFAULT
# ==================================
.DEFAULT_GOAL := help

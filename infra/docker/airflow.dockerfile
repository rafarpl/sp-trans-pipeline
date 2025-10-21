# ============================================================================
# AIRFLOW CUSTOM DOCKERFILE
# ============================================================================
# Build: docker build -f infra/docker/airflow.Dockerfile -t sptrans-airflow .
# ============================================================================

FROM apache/airflow:2.8.0-python3.11

# ============================================================================
# METADATA
# ============================================================================
LABEL maintainer="engenharia-dados@fia.br"
LABEL project="sptrans-realtime-pipeline"
LABEL version="1.0.0"
LABEL description="Apache Airflow customizado para pipeline SPTrans"

# ============================================================================
# USER ROOT - System packages
# ============================================================================
USER root

# Atualizar packages e instalar dependências do sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    g++ \
    libpq-dev \
    libssl-dev \
    libffi-dev \
    libgeos-dev \
    libproj-dev \
    curl \
    wget \
    vim \
    git \
    postgresql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Criar diretórios necessários
RUN mkdir -p /opt/airflow/dags \
    /opt/airflow/logs \
    /opt/airflow/plugins \
    /opt/airflow/config \
    /opt/airflow/src

# ============================================================================
# USER AIRFLOW - Python packages
# ============================================================================
USER airflow

# Copiar requirements
COPY requirements.txt /tmp/requirements.txt

# Instalar dependências Python
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r /tmp/requirements.txt

# Instalar providers adicionais do Airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.6.0 \
    apache-airflow-providers-amazon==8.15.0 \
    apache-airflow-providers-postgres==5.9.0 \
    apache-airflow-providers-redis==3.5.0 \
    apache-airflow-providers-celery==3.5.0

# Instalar PySpark e Delta Lake
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    delta-spark==3.0.0 \
    pyarrow==14.0.1

# Instalar bibliotecas adicionais
RUN pip install --no-cache-dir \
    boto3==1.34.19 \
    s3fs==2024.2.0 \
    pandas==2.1.4 \
    numpy==1.26.2 \
    requests==2.31.0 \
    geopy==2.4.1 \
    great-expectations==0.18.8 \
    prometheus-client==0.19.0 \
    python-dotenv==1.0.0

# ============================================================================
# HEALTHCHECK
# ============================================================================
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=5 \
    CMD airflow jobs check --job-type SchedulerJob --hostname "$${HOSTNAME}" || exit 1

# ============================================================================
# ENTRYPOINT & CMD
# ============================================================================
# Usar entrypoint padrão do Airflow
# CMD será definido no docker-compose.yml

# ============================================================================
# FIM DO DOCKERFILE
# ============================================================================
# =============================================================================
# APACHE AIRFLOW - DOCKERFILE
# =============================================================================
# Container customizado do Airflow para SPTrans Pipeline
# Base: Apache Airflow 2.8.1 oficial
# =============================================================================

FROM apache/airflow:2.8.1-python3.10

# Metadata
LABEL maintainer="rafael@sptrans-pipeline.com"
LABEL description="Apache Airflow for SPTrans Data Pipeline"
LABEL version="2.8.1"

# Switch to root para instalar dependências do sistema
USER root

# ============================================================================
# SYSTEM DEPENDENCIES
# ============================================================================

RUN apt-get update && apt-get install -y --no-install-recommends \
    # Build essentials
    build-essential \
    gcc \
    g++ \
    # Database clients
    postgresql-client \
    # Network tools
    curl \
    wget \
    netcat \
    # Java para Spark
    openjdk-11-jdk \
    # Git
    git \
    # Utils
    vim \
    && rm -rf /var/lib/apt/lists/*

# Configurar JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# ============================================================================
# AIRFLOW USER SETUP
# ============================================================================

# Voltar para usuário airflow
USER airflow

# ============================================================================
# PYTHON DEPENDENCIES
# ============================================================================

# Copiar requirements
COPY requirements.txt /tmp/requirements.txt

# Instalar dependências Python
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt

# ============================================================================
# ADDITIONAL AIRFLOW PROVIDERS
# ============================================================================

RUN pip install --no-cache-dir \
    # Spark
    apache-airflow-providers-apache-spark==4.5.0 \
    # PostgreSQL
    apache-airflow-providers-postgres==5.10.0 \
    # HTTP/API
    apache-airflow-providers-http==4.7.0 \
    # Amazon S3 (MinIO compatibility)
    apache-airflow-providers-amazon==8.15.0 \
    # Redis
    apache-airflow-providers-redis==3.5.0

# ============================================================================
# SPARK JARS
# ============================================================================

# Criar diretório para JARs
USER root
RUN mkdir -p /opt/spark/jars && \
    chown -R airflow:root /opt/spark/jars

USER airflow

# Download PostgreSQL JDBC driver
RUN wget -P /opt/spark/jars \
    https://jdbc.postgresql.org/download/postgresql-42.5.0.jar

# Download AWS SDK for Hadoop (MinIO)
RUN wget -P /opt/spark/jars \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -P /opt/spark/jars \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# ============================================================================
# AIRFLOW CONFIGURATION
# ============================================================================

# Configurações de ambiente
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
ENV AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True
ENV AIRFLOW__WEBSERVER__RBAC=True
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False

# Spark configuration
ENV SPARK_HOME=/usr/local/lib/python3.10/site-packages/pyspark
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# ============================================================================
# DIRECTORIES
# ============================================================================

# Criar diretórios necessários
USER root
RUN mkdir -p ${AIRFLOW_HOME}/dags \
             ${AIRFLOW_HOME}/logs \
             ${AIRFLOW_HOME}/plugins \
             ${AIRFLOW_HOME}/config \
             ${AIRFLOW_HOME}/src && \
    chown -R airflow:root ${AIRFLOW_HOME}

USER airflow

# ============================================================================
# WORKING DIRECTORY
# ============================================================================

WORKDIR ${AIRFLOW_HOME}

# ============================================================================
# HEALTHCHECK
# ============================================================================

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# ============================================================================
# ENTRYPOINT
# ============================================================================

# Usar entrypoint padrão do Airflow
# O comando será especificado no docker-compose.yml:
# - webserver
# - scheduler
# - worker (se CeleryExecutor)

# ============================================================================
# EXPOSE PORTS
# ============================================================================

# Webserver
EXPOSE 8080
# Flower (se CeleryExecutor)
EXPOSE 5555

# ============================================================================
# FINAL SETUP
# ============================================================================

# Copiar código fonte (será sobrescrito por volume no docker-compose)
# COPY src/ ${AIRFLOW_HOME}/src/
# COPY dags/ ${AIRFLOW_HOME}/dags/

# ============================================================================
# NOTES
# ============================================================================

# Para inicializar o banco de dados do Airflow:
#   docker-compose run airflow-init
#
# Para criar admin user:
#   docker exec -it airflow-webserver \
#     airflow users create \
#       --username admin \
#       --firstname Admin \
#       --lastname User \
#       --role Admin \
#       --email admin@example.com \
#       --password admin
#
# Para verificar configuração:
#   docker exec -it airflow-webserver airflow config list

# ============================================================================
# END
# ============================================================================

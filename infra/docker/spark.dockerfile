# =============================================================================
# APACHE SPARK - DOCKERFILE
# =============================================================================
# Container customizado do Spark para SPTrans Pipeline
# Base: Bitnami Spark 3.5
# =============================================================================

FROM bitnami/spark:3.5.0-debian-12-r3

# Metadata
LABEL maintainer="rafael@sptrans-pipeline.com"
LABEL description="Apache Spark for SPTrans Data Pipeline"
LABEL version="3.5.0"

# Switch to root
USER root

# ============================================================================
# SYSTEM DEPENDENCIES
# ============================================================================

RUN apt-get update && apt-get install -y --no-install-recommends \
    # Python
    python3-pip \
    python3-dev \
    # Build tools
    build-essential \
    gcc \
    g++ \
    # Database clients
    postgresql-client \
    # Network & utils
    curl \
    wget \
    netcat \
    vim \
    # For S3/MinIO
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# ============================================================================
# PYTHON PACKAGES
# ============================================================================

# Upgrade pip
RUN pip3 install --no-cache-dir --upgrade pip setuptools wheel

# Install Python dependencies for Spark jobs
RUN pip3 install --no-cache-dir \
    # Core
    pyspark==3.5.0 \
    pandas==2.1.4 \
    numpy==1.26.2 \
    # Database
    psycopg2-binary==2.9.9 \
    SQLAlchemy==2.0.23 \
    # S3/MinIO
    boto3==1.34.18 \
    s3fs==2024.2.0 \
    # Data formats
    pyarrow==14.0.2 \
    fastparquet==2023.10.1 \
    # Utils
    python-dotenv==1.0.0 \
    pyyaml==6.0.1 \
    requests==2.31.0 \
    # Geospatial (opcional)
    shapely==2.0.2

# ============================================================================
# SPARK JARS
# ============================================================================

# Criar diretório para JARs customizados
RUN mkdir -p /opt/spark/jars/custom && \
    chown -R 1001:0 /opt/spark/jars/custom

# Download PostgreSQL JDBC driver
RUN wget -P /opt/spark/jars/custom \
    https://jdbc.postgresql.org/download/postgresql-42.5.0.jar

# Download Hadoop AWS (para MinIO/S3)
RUN wget -P /opt/spark/jars/custom \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# Download AWS SDK bundle
RUN wget -P /opt/spark/jars/custom \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# ============================================================================
# SPARK CONFIGURATION
# ============================================================================

# Copiar configurações customizadas
# COPY config/spark/spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf
# COPY config/spark/log4j.properties /opt/bitnami/spark/conf/log4j.properties

# Environment variables
ENV SPARK_HOME=/opt/bitnami/spark
ENV SPARK_MASTER_HOST=spark-master
ENV SPARK_MASTER_PORT=7077
ENV SPARK_MASTER_WEBUI_PORT=8081

# Python
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV PYTHONPATH=/opt/bitnami/spark/python:/opt/bitnami/spark/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

# Memory settings
ENV SPARK_DRIVER_MEMORY=2g
ENV SPARK_EXECUTOR_MEMORY=4g
ENV SPARK_WORKER_MEMORY=4g
ENV SPARK_WORKER_CORES=2

# S3/MinIO configuration
ENV AWS_ACCESS_KEY_ID=admin
ENV AWS_SECRET_ACCESS_KEY=miniopassword123
ENV AWS_REGION=us-east-1

# ============================================================================
# WORKING DIRECTORY
# ============================================================================

WORKDIR /app

# Criar estrutura de diretórios
RUN mkdir -p /app/src \
             /app/dags \
             /app/logs \
             /app/tmp && \
    chown -R 1001:0 /app

# ============================================================================
# APPLICATION CODE
# ============================================================================

# Código será montado via volume no docker-compose
# COPY src/ /app/src/
# COPY dags/ /app/dags/

# ============================================================================
# PERMISSIONS
# ============================================================================

# Ajustar permissões
RUN chmod -R 775 /opt/bitnami/spark && \
    chmod -R 775 /app

# ============================================================================
# USER
# ============================================================================

# Voltar para usuário não-root
USER 1001

# ============================================================================
# HEALTHCHECK
# ============================================================================

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8081/ || exit 1

# ============================================================================
# EXPOSE PORTS
# ============================================================================

# Master Web UI
EXPOSE 8081
# Master RPC
EXPOSE 7077
# Worker Web UI
EXPOSE 8082
# Driver
EXPOSE 4040

# ============================================================================
# ENTRYPOINT
# ============================================================================

# Usar entrypoint padrão do Bitnami
# O comando será especificado no docker-compose.yml:
# - Master mode: /opt/bitnami/scripts/spark/run.sh
# - Worker mode: /opt/bitnami/scripts/spark/run.sh

# ============================================================================
# USAGE EXAMPLES
# ============================================================================

# Submit a Spark job:
#   docker exec spark-master spark-submit \
#     --master spark://spark-master:7077 \
#     --deploy-mode client \
#     /app/src/processing/jobs/bronze_to_silver.py
#
# Access Spark shell:
#   docker exec -it spark-master spark-shell
#
# Access PySpark shell:
#   docker exec -it spark-master pyspark

# ============================================================================
# END
# ============================================================================

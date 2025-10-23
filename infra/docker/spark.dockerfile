# ============================================================================
# SPARK CUSTOM DOCKERFILE
# ============================================================================
# Build: docker build -f infra/docker/spark.Dockerfile -t sptrans-spark .
# ============================================================================

FROM bitnami/spark:3.5.0

# ============================================================================
# METADATA
# ============================================================================
LABEL maintainer="engenharia-dados@fia.br"
LABEL project="sptrans-realtime-pipeline"
LABEL version="1.0.0"
LABEL description="Apache Spark customizado para pipeline SPTrans"

# ============================================================================
# USER ROOT - System packages
# ============================================================================
USER root

# Instalar dependências do sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    wget \
    vim \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ============================================================================
# SPARK JARS - Download dependencies
# ============================================================================
WORKDIR /opt/bitnami/spark/jars

# AWS SDK para MinIO/S3
RUN wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Delta Lake
RUN wget -q https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.0.0/delta-spark_2.12-3.0.0.jar && \
    wget -q https://repo1.maven.org/maven2/io/delta/delta-storage/3.0.0/delta-storage-3.0.0.jar

# PostgreSQL JDBC Driver
RUN wget -q https://jdbc.postgresql.org/download/postgresql-42.7.1.jar

# ============================================================================
# PYTHON PACKAGES
# ============================================================================
USER 1001

# Atualizar pip
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Instalar dependências Python para Spark
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    delta-spark==3.0.0 \
    pyarrow==14.0.1 \
    pandas==2.1.4 \
    numpy==1.26.2 \
    boto3==1.34.19 \
    s3fs==2024.2.0 \
    requests==2.31.0 \
    geopy==2.4.1 \
    shapely==2.0.2 \
    python-dotenv==1.0.0 \
    pydantic==2.5.3

# ============================================================================
# SPARK CONFIGURATION
# ============================================================================
# Criar diretórios para aplicações
RUN mkdir -p /opt/spark-apps /opt/spark-data

# Copiar spark-defaults.conf (será substituído por volume no docker-compose)
# COPY config/spark/spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf

# ============================================================================
# ENVIRONMENT VARIABLES
# ============================================================================
ENV SPARK_HOME=/opt/bitnami/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Delta Lake configuration
ENV SPARK_SQL_EXTENSIONS=io.delta.sql.DeltaSparkSessionExtension
ENV SPARK_SQL_CATALOG_SPARK_CATALOG=org.apache.spark.sql.delta.catalog.DeltaCatalog

# ============================================================================
# HEALTHCHECK
# ============================================================================
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8081 || exit 1

# ============================================================================
# WORKDIR
# ============================================================================
WORKDIR /opt/spark-apps

# ============================================================================
# ENTRYPOINT
# ============================================================================
# Usar entrypoint padrão do Bitnami Spark
# CMD será definido no docker-compose.yml (master ou worker)

# ============================================================================
# FIM DO DOCKERFILE
# ============================================================================
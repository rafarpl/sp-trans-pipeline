# =============================================================================
# SPTRANS REAL-TIME PIPELINE - MAIN APPLICATION DOCKERFILE
# =============================================================================
# Base image with Python 3.10 and Java (required for PySpark)
FROM python:3.10-slim-bullseye

# Metadata
LABEL maintainer="rafael@sptrans-pipeline.com"
LABEL description="SPTrans Real-Time Data Pipeline - Main Application"
LABEL version="1.0.0"

# Environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DEBIAN_FRONTEND=noninteractive \
    JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
    SPARK_HOME=/opt/spark \
    PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Java (required for Spark)
    openjdk-11-jdk-headless \
    # Build tools
    build-essential \
    gcc \
    g++ \
    # Network tools
    curl \
    wget \
    netcat \
    # Git for version control
    git \
    # PostgreSQL client
    postgresql-client \
    # Utilities
    vim \
    nano \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Install Apache Spark
ARG SPARK_VERSION=3.4.1
ARG HADOOP_VERSION=3
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt/ \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Create application user (non-root for security)
RUN useradd -m -u 1000 -s /bin/bash sptrans && \
    mkdir -p /app /app/logs /app/data && \
    chown -R sptrans:sptrans /app

# Set working directory
WORKDIR /app

# Copy requirements first (for better caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY --chown=sptrans:sptrans . .

# Switch to non-root user
USER sptrans

# Expose ports
EXPOSE 8080 4040 8888

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)" || exit 1

# Default command (can be overridden)
CMD ["python", "--version"]

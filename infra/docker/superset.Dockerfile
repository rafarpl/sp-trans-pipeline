# =============================================================================
# APACHE SUPERSET - DOCKERFILE
# =============================================================================
# Dashboard e visualização de dados para SPTrans Pipeline
# Base: Apache Superset oficial

FROM apache/superset:3.0.0

# Metadata
LABEL maintainer="sptrans-data-team"
LABEL description="Apache Superset for SPTrans Dashboard"
LABEL version="3.0.0"

# Switch to root para instalar dependências
USER root

# Instalar dependências adicionais
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Instalar drivers de banco de dados
RUN pip install --no-cache-dir \
    psycopg2-binary==2.9.9 \
    sqlalchemy-redshift==0.8.14 \
    redis==5.0.1 \
    celery==5.3.4

# Instalar bibliotecas adicionais para visualizações
RUN pip install --no-cache-dir \
    plotly==5.18.0 \
    pandas==2.1.4 \
    geopy==2.4.1 \
    pydeck==0.8.1

# Criar diretório para configurações customizadas
RUN mkdir -p /app/superset_config

# Copiar configuração customizada (se existir)
# COPY superset_config.py /app/superset_config/

# Configurar variáveis de ambiente
ENV SUPERSET_CONFIG_PATH=/app/superset_config/superset_config.py
ENV FLASK_APP=superset
ENV SUPERSET_SECRET_KEY='sptrans-superset-secret-key-change-me'

# Voltar para usuário superset
USER superset

# Porta padrão
EXPOSE 8088

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8088/health || exit 1

# Comando padrão
CMD ["gunicorn", \
     "--bind", "0.0.0.0:8088", \
     "--workers", "4", \
     "--timeout", "120", \
     "--limit-request-line", "0", \
     "--limit-request-field_size", "0", \
     "superset.app:create_app()"]

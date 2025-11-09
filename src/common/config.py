"""
Configuration Module

Configuração centralizada usando Pydantic Settings.
"""

import os
from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    """
    Configuração principal do projeto.
    
    Carrega variáveis de ambiente do arquivo .env
    """
    
    # ============================================================================
    # POSTGRESQL
    # ============================================================================
    POSTGRES_HOST: str = Field(default="localhost", alias="postgres_host")
    POSTGRES_PORT: int = Field(default=5432, alias="postgres_port")
    POSTGRES_DB: str = Field(default="sptrans_test", alias="postgres_db")
    POSTGRES_USER: str = Field(default="test_user", alias="postgres_user")
    POSTGRES_PASSWORD: str = Field(default="test_password", alias="postgres_password")
    
    # ============================================================================
    # REDIS
    # ============================================================================
    REDIS_HOST: str = Field(default="localhost", alias="redis_host")
    REDIS_PORT: int = Field(default=6379, alias="redis_port")
    REDIS_PASSWORD: Optional[str] = Field(default=None, alias="redis_password")
    
    # ============================================================================
    # KAFKA
    # ============================================================================
    KAFKA_BOOTSTRAP_SERVERS: str = Field(
        default="localhost:9092",
        alias="kafka_bootstrap_servers"
    )
    
    # ============================================================================
    # MINIO (S3-compatible)
    # ============================================================================
    MINIO_ENDPOINT: str = Field(default="localhost:9000", alias="minio_endpoint")
    MINIO_ACCESS_KEY: str = Field(default="minioadmin", alias="minio_access_key")
    MINIO_SECRET_KEY: str = Field(default="minioadmin", alias="minio_secret_key")
    MINIO_BUCKET: str = Field(default="sptrans-datalake", alias="minio_bucket")
    
    # ============================================================================
    # SPTRANS API
    # ============================================================================
    SPTRANS_API_TOKEN: str = Field(default="82b8355f050a80ab2faa0ec492e3d5ed52369723e3fd2bb0c4b37d55bfa01cec", alias="sptrans_api_token")
    SPTRANS_API_BASE_URL: str = Field(
        default="http://api.olhovivo.sptrans.com.br/v2.1",
        alias="sptrans_api_base_url"
    )
    
    # ============================================================================
    # SPARK / JAVA
    # ============================================================================
    JAVA_HOME: str = Field(
        default="/usr/lib/jvm/java-11-openjdk-amd64",
        alias="java_home"
    )
    SPARK_HOME: Optional[str] = Field(default=None, alias="spark_home")
    
    # ============================================================================
    # CONFIGURAÇÃO DO PYDANTIC
    # ============================================================================
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",  # CRÍTICO: Ignora campos extras ao invés de erro
        populate_by_name=True
    )


# Singleton
_config_instance: Optional[Config] = None


def get_config() -> Config:
    """
    Obtém instância singleton da configuração.
    
    Returns:
        Config: Instância única da configuração
    """
    global _config_instance
    
    if _config_instance is None:
        _config_instance = Config()
    
    return _config_instance


# Limpar cache se necessário
def reset_config():
    """Reseta o singleton (útil para testes)."""
    global _config_instance
    _config_instance = None

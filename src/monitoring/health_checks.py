"""
Health Checks Module

Responsável por verificações de saúde do sistema:
- Status dos serviços (Kafka, Postgres, Redis, MinIO)
- Conectividade
- Performance checks
- Alertas de problemas
"""

from typing import Dict, List, Optional
from datetime import datetime
from enum import Enum
import time

import psycopg2
import redis
from kafka import KafkaProducer
from minio import Minio

from src.common.config import get_config
from src.common.logging_config import get_logger
from src.common.exceptions import SPTransPipelineException

logger = get_logger(__name__)


class HealthStatus(Enum):
    """Status de saúde de um componente."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class HealthCheckResult:
    """Resultado de um health check."""
    
    def __init__(
        self,
        component: str,
        status: HealthStatus,
        message: str = "",
        response_time_ms: float = 0.0,
        details: Optional[Dict] = None
    ):
        self.component = component
        self.status = status
        self.message = message
        self.response_time_ms = response_time_ms
        self.details = details or {}
        self.timestamp = datetime.now()
    
    def to_dict(self) -> Dict:
        """Converte para dicionário."""
        return {
            "component": self.component,
            "status": self.status.value,
            "message": self.message,
            "response_time_ms": self.response_time_ms,
            "details": self.details,
            "timestamp": self.timestamp.isoformat()
        }
    
    def is_healthy(self) -> bool:
        """Verifica se o componente está saudável."""
        return self.status == HealthStatus.HEALTHY


class HealthChecker:
    """
    Classe para executar health checks nos componentes do sistema.
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """
        Inicializa o HealthChecker.
        
        Args:
            config: Configuração opcional
        """
        self.config = config or get_config()
        self.logger = get_logger(self.__class__.__name__)
    
    def check_postgres(self) -> HealthCheckResult:
        """
        Verifica saúde do PostgreSQL.
        
        Returns:
            HealthCheckResult
        """
        self.logger.info("Checking PostgreSQL health")
        
        start_time = time.time()
        
        try:
            # Tentar conectar
            conn = psycopg2.connect(
                host=self.config.POSTGRES_HOST,
                port=self.config.POSTGRES_PORT,
                database=self.config.POSTGRES_DB,
                user=self.config.POSTGRES_USER,
                password=self.config.POSTGRES_PASSWORD,
                connect_timeout=5
            )
            
            # Executar query simples
            cursor = conn.cursor()
            cursor.execute("SELECT 1;")
            cursor.fetchone()
            cursor.close()
            conn.close()
            
            response_time = (time.time() - start_time) * 1000
            
            return HealthCheckResult(
                component="PostgreSQL",
                status=HealthStatus.HEALTHY,
                message="Connected successfully",
                response_time_ms=response_time
            )
            
        except psycopg2.OperationalError as e:
            response_time = (time.time() - start_time) * 1000
            
            return HealthCheckResult(
                component="PostgreSQL",
                status=HealthStatus.UNHEALTHY,
                message=f"Connection failed: {str(e)[:100]}",
                response_time_ms=response_time
            )
        
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            
            return HealthCheckResult(
                component="PostgreSQL",
                status=HealthStatus.UNKNOWN,
                message=f"Unexpected error: {str(e)[:100]}",
                response_time_ms=response_time
            )
    
    def check_redis(self) -> HealthCheckResult:
        """
        Verifica saúde do Redis.
        
        Returns:
            HealthCheckResult
        """
        self.logger.info("Checking Redis health")
        
        start_time = time.time()
        
        try:
            # Conectar ao Redis
            client = redis.Redis(
                host=self.config.REDIS_HOST,
                port=self.config.REDIS_PORT,
                password=self.config.REDIS_PASSWORD,
                socket_connect_timeout=5,
                socket_timeout=5
            )
            
            # Ping
            client.ping()
            
            # Obter info
            info = client.info()
            
            response_time = (time.time() - start_time) * 1000
            
            return HealthCheckResult(
                component="Redis",
                status=HealthStatus.HEALTHY,
                message="Connected successfully",
                response_time_ms=response_time,
                details={
                    "used_memory_mb": info.get("used_memory", 0) / 1024 / 1024,
                    "connected_clients": info.get("connected_clients", 0)
                }
            )
            
        except redis.exceptions.ConnectionError as e:
            response_time = (time.time() - start_time) * 1000
            
            return HealthCheckResult(
                component="Redis",
                status=HealthStatus.UNHEALTHY,
                message=f"Connection failed: {str(e)[:100]}",
                response_time_ms=response_time
            )
        
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            
            return HealthCheckResult(
                component="Redis",
                status=HealthStatus.UNKNOWN,
                message=f"Unexpected error: {str(e)[:100]}",
                response_time_ms=response_time
            )
    
    def check_kafka(self) -> HealthCheckResult:
        """
        Verifica saúde do Kafka.
        
        Returns:
            HealthCheckResult
        """
        self.logger.info("Checking Kafka health")
        
        start_time = time.time()
        
        try:
            # Tentar criar producer
            producer = KafkaProducer(
                bootstrap_servers=self.config.KAFKA_BOOTSTRAP_SERVERS,
                request_timeout_ms=5000,
                api_version_auto_timeout_ms=5000
            )
            
            # Listar tópicos
            topics = producer.list_topics(timeout=5)
            
            producer.close()
            
            response_time = (time.time() - start_time) * 1000
            
            return HealthCheckResult(
                component="Kafka",
                status=HealthStatus.HEALTHY,
                message="Connected successfully",
                response_time_ms=response_time,
                details={
                    "topics_count": len(topics.topics) if topics else 0
                }
            )
            
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            
            return HealthCheckResult(
                component="Kafka",
                status=HealthStatus.UNHEALTHY,
                message=f"Connection failed: {str(e)[:100]}",
                response_time_ms=response_time
            )
    
    def check_minio(self) -> HealthCheckResult:
        """
        Verifica saúde do MinIO.
        
        Returns:
            HealthCheckResult
        """
        self.logger.info("Checking MinIO health")
        
        start_time = time.time()
        
        try:
            # Conectar ao MinIO
            client = Minio(
                self.config.MINIO_ENDPOINT,
                access_key=self.config.MINIO_ACCESS_KEY,
                secret_key=self.config.MINIO_SECRET_KEY,
                secure=False  # Use True em produção com HTTPS
            )
            
            # Listar buckets
            buckets = client.list_buckets()
            
            response_time = (time.time() - start_time) * 1000
            
            return HealthCheckResult(
                component="MinIO",
                status=HealthStatus.HEALTHY,
                message="Connected successfully",
                response_time_ms=response_time,
                details={
                    "buckets_count": len(buckets)
                }
            )
            
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            
            return HealthCheckResult(
                component="MinIO",
                status=HealthStatus.UNHEALTHY,
                message=f"Connection failed: {str(e)[:100]}",
                response_time_ms=response_time
            )
    
    def check_all(self) -> Dict[str, HealthCheckResult]:
        """
        Executa health checks em todos os componentes.
        
        Returns:
            Dicionário com resultados por componente
        """
        self.logger.info("Starting health checks for all components")
        
        results = {
            "postgres": self.check_postgres(),
            "redis": self.check_redis(),
            "kafka": self.check_kafka(),
            "minio": self.check_minio()
        }
        
        # Log resumo
        healthy_count = sum(1 for r in results.values() if r.is_healthy())
        total_count = len(results)
        
        self.logger.info(
            f"Health check completed: {healthy_count}/{total_count} components healthy"
        )
        
        return results
    
    def get_system_health_summary(self) -> Dict:
        """
        Obtém resumo da saúde do sistema.
        
        Returns:
            Dicionário com resumo
        """
        results = self.check_all()
        
        # Determinar status geral
        all_healthy = all(r.is_healthy() for r in results.values())
        any_unhealthy = any(r.status == HealthStatus.UNHEALTHY for r in results.values())
        
        if all_healthy:
            overall_status = HealthStatus.HEALTHY
        elif any_unhealthy:
            overall_status = HealthStatus.DEGRADED
        else:
            overall_status = HealthStatus.UNKNOWN
        
        # Construir resumo
        summary = {
            "overall_status": overall_status.value,
            "timestamp": datetime.now().isoformat(),
            "components": {
                name: result.to_dict()
                for name, result in results.items()
            }
        }
        
        return summary


def check_system_health() -> Dict:
    """
    Função utilitária para verificar saúde do sistema.
    
    Returns:
        Dicionário com resumo de saúde
    """
    checker = HealthChecker()
    return checker.get_system_health_summary()


def is_system_healthy() -> bool:
    """
    Verifica se o sistema está saudável (todos os componentes OK).
    
    Returns:
        True se todos os componentes estão saudáveis
    """
    checker = HealthChecker()
    results = checker.check_all()
    return all(r.is_healthy() for r in results.values())
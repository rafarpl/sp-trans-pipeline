# =============================================================================
# HEALTH CHECKER
# =============================================================================
# Verificação de saúde dos componentes do sistema
# =============================================================================

"""
Health Checker

Verifica saúde e disponibilidade dos componentes:
    - Serviços: PostgreSQL, Redis, Kafka, MinIO
    - Pipeline: Airflow, Spark
    - APIs: SPTrans Olho Vivo
    - Infraestrutura: CPU, memória, disco
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from enum import Enum

# =============================================================================
# LOGGING
# =============================================================================

logger = logging.getLogger(__name__)

# =============================================================================
# ENUMS
# =============================================================================

class HealthStatus(str, Enum):
    """Status de saúde"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"

# =============================================================================
# HEALTH CHECKER
# =============================================================================

class HealthChecker:
    """Verificador de saúde do sistema"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Inicializa health checker.
        
        Args:
            config: Configurações opcionais
        """
        self.config = config or {}
        
        # Timeouts
        self.connection_timeout = self.config.get("connection_timeout", 5)
        
        # Cache de status (para evitar checks muito frequentes)
        self._status_cache: Dict[str, Dict[str, Any]] = {}
        self._cache_ttl = self.config.get("cache_ttl", 60)  # 1 minuto
        
        logger.info("HealthChecker inicializado")
    
    def check_all(self) -> Dict[str, Any]:
        """
        Verifica saúde de todos os componentes.
        
        Returns:
            Dicionário com status de todos os componentes
        
        Example:
            >>> checker = HealthChecker()
            >>> health = checker.check_all()
            >>> print(health["overall_status"])
        """
        logger.info("Verificando saúde de todos os componentes...")
        
        health = {
            "timestamp": datetime.now().isoformat(),
            "components": {},
            "overall_status": HealthStatus.UNKNOWN.value,
        }
        
        # Verificar cada componente
        components = [
            ("postgres", self.check_postgres),
            ("redis", self.check_redis),
            ("minio", self.check_minio),
            ("kafka", self.check_kafka),
            ("spark", self.check_spark),
            ("airflow", self.check_airflow),
            ("api_sptrans", self.check_api_sptrans),
        ]
        
        for name, check_func in components:
            try:
                health["components"][name] = check_func()
            except Exception as e:
                logger.error(f"Erro ao verificar {name}: {e}")
                health["components"][name] = {
                    "status": HealthStatus.UNKNOWN.value,
                    "error": str(e)
                }
        
        # Calcular status geral
        health["overall_status"] = self._calculate_overall_status(
            health["components"]
        )
        
        # Adicionar contadores
        health["summary"] = self._summarize_health(health["components"])
        
        return health
    
    def check_postgres(self) -> Dict[str, Any]:
        """Verifica PostgreSQL"""
        logger.debug("Checking PostgreSQL...")
        
        try:
            import psycopg2
            import os
            
            conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "postgres"),
                port=int(os.getenv("POSTGRES_PORT", "5432")),
                database=os.getenv("POSTGRES_DB", "airflow"),
                user=os.getenv("POSTGRES_USER", "airflow"),
                password=os.getenv("POSTGRES_PASSWORD", "airflow123"),
                connect_timeout=self.connection_timeout
            )
            
            # Test query
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            conn.close()
            
            return {
                "status": HealthStatus.HEALTHY.value,
                "message": "PostgreSQL is reachable",
                "response_time_ms": None
            }
        
        except Exception as e:
            logger.error(f"PostgreSQL check failed: {e}")
            return {
                "status": HealthStatus.UNHEALTHY.value,
                "message": f"PostgreSQL unreachable: {str(e)}"
            }
    
    def check_redis(self) -> Dict[str, Any]:
        """Verifica Redis"""
        logger.debug("Checking Redis...")
        
        try:
            import redis
            import os
            import time
            
            start = time.time()
            
            client = redis.Redis(
                host=os.getenv("REDIS_HOST", "redis"),
                port=int(os.getenv("REDIS_PORT", "6379")),
                db=0,
                socket_timeout=self.connection_timeout,
                decode_responses=True
            )
            
            # Ping
            client.ping()
            
            # Info
            info = client.info()
            
            response_time = (time.time() - start) * 1000
            
            return {
                "status": HealthStatus.HEALTHY.value,
                "message": "Redis is reachable",
                "response_time_ms": round(response_time, 2),
                "keys_count": client.dbsize(),
                "memory_used_mb": round(info.get("used_memory", 0) / 1024 / 1024, 2)
            }
        
        except Exception as e:
            logger.error(f"Redis check failed: {e}")
            return {
                "status": HealthStatus.UNHEALTHY.value,
                "message": f"Redis unreachable: {str(e)}"
            }
    
    def check_minio(self) -> Dict[str, Any]:
        """Verifica MinIO/S3"""
        logger.debug("Checking MinIO...")
        
        try:
            import boto3
            import os
            import time
            
            start = time.time()
            
            s3_client = boto3.client(
                's3',
                endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
                aws_access_key_id=os.getenv("MINIO_ROOT_USER", "admin"),
                aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "miniopassword123"),
                region_name='us-east-1'
            )
            
            # List buckets
            buckets = s3_client.list_buckets()
            
            response_time = (time.time() - start) * 1000
            
            return {
                "status": HealthStatus.HEALTHY.value,
                "message": "MinIO is reachable",
                "response_time_ms": round(response_time, 2),
                "buckets_count": len(buckets.get("Buckets", []))
            }
        
        except Exception as e:
            logger.error(f"MinIO check failed: {e}")
            return {
                "status": HealthStatus.UNHEALTHY.value,
                "message": f"MinIO unreachable: {str(e)}"
            }
    
    def check_kafka(self) -> Dict[str, Any]:
        """Verifica Kafka"""
        logger.debug("Checking Kafka...")
        
        try:
            from kafka import KafkaAdminClient
            import os
            import time
            
            start = time.time()
            
            admin_client = KafkaAdminClient(
                bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
                request_timeout_ms=self.connection_timeout * 1000
            )
            
            # List topics
            topics = admin_client.list_topics()
            
            admin_client.close()
            
            response_time = (time.time() - start) * 1000
            
            return {
                "status": HealthStatus.HEALTHY.value,
                "message": "Kafka is reachable",
                "response_time_ms": round(response_time, 2),
                "topics_count": len(topics)
            }
        
        except Exception as e:
            logger.warning(f"Kafka check failed (may be disabled): {e}")
            return {
                "status": HealthStatus.UNKNOWN.value,
                "message": f"Kafka check failed: {str(e)}"
            }
    
    def check_spark(self) -> Dict[str, Any]:
        """Verifica Spark"""
        logger.debug("Checking Spark...")
        
        try:
            import requests
            import os
            import time
            
            start = time.time()
            
            spark_master_url = os.getenv("SPARK_MASTER_URL", "http://spark-master:8080")
            
            response = requests.get(
                f"{spark_master_url}/json",
                timeout=self.connection_timeout
            )
            response.raise_for_status()
            
            data = response.json()
            
            response_time = (time.time() - start) * 1000
            
            return {
                "status": HealthStatus.HEALTHY.value,
                "message": "Spark master is reachable",
                "response_time_ms": round(response_time, 2),
                "workers": data.get("aliveworkers", 0),
                "cores_total": data.get("cores", 0)
            }
        
        except Exception as e:
            logger.error(f"Spark check failed: {e}")
            return {
                "status": HealthStatus.DEGRADED.value,
                "message": f"Spark check failed: {str(e)}"
            }
    
    def check_airflow(self) -> Dict[str, Any]:
        """Verifica Airflow"""
        logger.debug("Checking Airflow...")
        
        try:
            import requests
            import os
            import time
            
            start = time.time()
            
            airflow_url = os.getenv("AIRFLOW_URL", "http://airflow-webserver:8080")
            
            response = requests.get(
                f"{airflow_url}/health",
                timeout=self.connection_timeout
            )
            
            response_time = (time.time() - start) * 1000
            
            if response.status_code == 200:
                data = response.json()
                
                return {
                    "status": HealthStatus.HEALTHY.value,
                    "message": "Airflow is healthy",
                    "response_time_ms": round(response_time, 2),
                    "metadatabase": data.get("metadatabase", {}).get("status"),
                    "scheduler": data.get("scheduler", {}).get("status")
                }
            else:
                return {
                    "status": HealthStatus.DEGRADED.value,
                    "message": f"Airflow returned status {response.status_code}"
                }
        
        except Exception as e:
            logger.error(f"Airflow check failed: {e}")
            return {
                "status": HealthStatus.UNHEALTHY.value,
                "message": f"Airflow unreachable: {str(e)}"
            }
    
    def check_api_sptrans(self) -> Dict[str, Any]:
        """Verifica API SPTrans"""
        logger.debug("Checking SPTrans API...")
        
        try:
            import requests
            import time
            
            start = time.time()
            
            # Apenas verificar se o endpoint está acessível
            # (não fazer autenticação para evitar rate limiting)
            response = requests.get(
                "http://api.olhovivo.sptrans.com.br/v2.1",
                timeout=self.connection_timeout
            )
            
            response_time = (time.time() - start) * 1000
            
            # API retorna 401 se não autenticado, mas está online
            if response.status_code in [200, 401]:
                return {
                    "status": HealthStatus.HEALTHY.value,
                    "message": "SPTrans API is reachable",
                    "response_time_ms": round(response_time, 2)
                }
            else:
                return {
                    "status": HealthStatus.DEGRADED.value,
                    "message": f"SPTrans API returned status {response.status_code}"
                }
        
        except Exception as e:
            logger.error(f"SPTrans API check failed: {e}")
            return {
                "status": HealthStatus.UNHEALTHY.value,
                "message": f"SPTrans API unreachable: {str(e)}"
            }
    
    def check_component(self, component_name: str) -> Dict[str, Any]:
        """
        Verifica componente específico.
        
        Args:
            component_name: Nome do componente
        
        Returns:
            Status do componente
        """
        check_map = {
            "postgres": self.check_postgres,
            "redis": self.check_redis,
            "minio": self.check_minio,
            "kafka": self.check_kafka,
            "spark": self.check_spark,
            "airflow": self.check_airflow,
            "api_sptrans": self.check_api_sptrans,
        }
        
        check_func = check_map.get(component_name)
        
        if check_func:
            return check_func()
        else:
            return {
                "status": HealthStatus.UNKNOWN.value,
                "message": f"Unknown component: {component_name}"
            }
    
    def _calculate_overall_status(
        self,
        components: Dict[str, Dict[str, Any]]
    ) -> str:
        """Calcula status geral baseado nos componentes"""
        
        statuses = [comp["status"] for comp in components.values()]
        
        # Se qualquer componente crítico está unhealthy
        critical = ["postgres", "redis", "minio"]
        critical_statuses = [
            components[c]["status"]
            for c in critical
            if c in components
        ]
        
        if HealthStatus.UNHEALTHY.value in critical_statuses:
            return HealthStatus.UNHEALTHY.value
        
        # Se algum componente está unhealthy
        if HealthStatus.UNHEALTHY.value in statuses:
            return HealthStatus.DEGRADED.value
        
        # Se algum componente está degraded
        if HealthStatus.DEGRADED.value in statuses:
            return HealthStatus.DEGRADED.value
        
        # Se todos estão healthy
        if all(s == HealthStatus.HEALTHY.value for s in statuses if s != HealthStatus.UNKNOWN.value):
            return HealthStatus.HEALTHY.value
        
        return HealthStatus.UNKNOWN.value
    
    def _summarize_health(
        self,
        components: Dict[str, Dict[str, Any]]
    ) -> Dict[str, int]:
        """Sumariza contadores de status"""
        
        summary = {
            "total": len(components),
            "healthy": 0,
            "degraded": 0,
            "unhealthy": 0,
            "unknown": 0,
        }
        
        for comp in components.values():
            status = comp.get("status", HealthStatus.UNKNOWN.value)
            if status == HealthStatus.HEALTHY.value:
                summary["healthy"] += 1
            elif status == HealthStatus.DEGRADED.value:
                summary["degraded"] += 1
            elif status == HealthStatus.UNHEALTHY.value:
                summary["unhealthy"] += 1
            else:
                summary["unknown"] += 1
        
        return summary


# =============================================================================
# FACTORY FUNCTION
# =============================================================================

def create_health_checker(config: Optional[Dict[str, Any]] = None) -> HealthChecker:
    """
    Factory function para criar HealthChecker.
    
    Args:
        config: Configurações
    
    Returns:
        HealthChecker configurado
    
    Example:
        >>> checker = create_health_checker()
        >>> health = checker.check_all()
    """
    return HealthChecker(config)


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def check_system_health() -> Dict[str, Any]:
    """
    Atalho para verificar saúde do sistema.
    
    Returns:
        Status de saúde
    """
    checker = create_health_checker()
    return checker.check_all()


def get_component_status(component_name: str) -> Dict[str, Any]:
    """
    Atalho para obter status de componente específico.
    
    Args:
        component_name: Nome do componente
    
    Returns:
        Status do componente
    """
    checker = create_health_checker()
    return checker.check_component(component_name)


# =============================================================================
# MAIN (para testes)
# =============================================================================

def main():
    """Função main para testes"""
    import json
    
    logger.info("=== Testando Health Checker ===")
    
    # Criar checker
    checker = create_health_checker()
    
    # Check all
    logger.info("\n1. Verificando todos os componentes")
    health = checker.check_all()
    
    print(json.dumps(health, indent=2))
    
    # Summary
    logger.info(f"\n2. Overall Status: {health['overall_status'].upper()}")
    logger.info(f"Healthy: {health['summary']['healthy']}")
    logger.info(f"Degraded: {health['summary']['degraded']}")
    logger.info(f"Unhealthy: {health['summary']['unhealthy']}")
    
    logger.info("\n✓ Health check concluído")


if __name__ == "__main__":
    main()

# =============================================================================
# END
# =============================================================================

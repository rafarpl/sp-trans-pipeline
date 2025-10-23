"""
Metrics Collection Module
=========================
Coleta e exposição de métricas para monitoramento do pipeline SPTrans.

Integração com Prometheus para observabilidade.
"""

from typing import Dict, Any, Optional
from datetime import datetime
import time
from functools import wraps
from collections import defaultdict
import threading

from prometheus_client import (
    Counter, Gauge, Histogram, Summary,
    CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST
)

from src.common.logging_config import get_logger

logger = get_logger(__name__)


class MetricsCollector:
    """Coletor central de métricas do sistema."""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        """Singleton pattern para garantir única instância."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """Inicializa coletor de métricas."""
        if self._initialized:
            return
        
        self.registry = CollectorRegistry()
        self._initialize_metrics()
        self._initialized = True
        logger.info("MetricsCollector inicializado")
    
    def _initialize_metrics(self):
        """Inicializa todas as métricas."""
        
        # === MÉTRICAS DE API SPTRANS ===
        self.api_requests_total = Counter(
            'sptrans_api_requests_total',
            'Total de requisições à API SPTrans',
            ['endpoint', 'status'],
            registry=self.registry
        )
        
        self.api_request_duration = Histogram(
            'sptrans_api_request_duration_seconds',
            'Duração das requisições à API SPTrans',
            ['endpoint'],
            registry=self.registry,
            buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0)
        )
        
        self.api_errors_total = Counter(
            'sptrans_api_errors_total',
            'Total de erros na API SPTrans',
            ['endpoint', 'error_type'],
            registry=self.registry
        )
        
        self.api_rate_limit_hits = Counter(
            'sptrans_api_rate_limit_hits_total',
            'Número de vezes que atingiu rate limit',
            registry=self.registry
        )
        
        # === MÉTRICAS DE DADOS ===
        self.records_processed = Counter(
            'sptrans_records_processed_total',
            'Total de registros processados',
            ['layer', 'source'],
            registry=self.registry
        )
        
        self.records_invalid = Counter(
            'sptrans_records_invalid_total',
            'Total de registros inválidos',
            ['layer', 'reason'],
            registry=self.registry
        )
        
        self.records_duplicated = Counter(
            'sptrans_records_duplicated_total',
            'Total de registros duplicados removidos',
            ['layer'],
            registry=self.registry
        )
        
        self.data_quality_score = Gauge(
            'sptrans_data_quality_score',
            'Score de qualidade dos dados (0-100)',
            ['layer', 'metric'],
            registry=self.registry
        )
        
        self.active_vehicles = Gauge(
            'sptrans_active_vehicles',
            'Número de veículos ativos',
            ['route'],
            registry=self.registry
        )
        
        self.active_routes = Gauge(
            'sptrans_active_routes',
            'Número de rotas ativas',
            registry=self.registry
        )
        
        # === MÉTRICAS DE PROCESSAMENTO ===
        self.job_duration = Histogram(
            'sptrans_job_duration_seconds',
            'Duração dos jobs de processamento',
            ['job_name', 'status'],
            registry=self.registry,
            buckets=(1, 5, 10, 30, 60, 120, 300, 600, 1800)
        )
        
        self.job_executions = Counter(
            'sptrans_job_executions_total',
            'Total de execuções de jobs',
            ['job_name', 'status'],
            registry=self.registry
        )
        
        self.spark_tasks_total = Counter(
            'sptrans_spark_tasks_total',
            'Total de tasks Spark executadas',
            ['job_name', 'stage'],
            registry=self.registry
        )
        
        self.spark_memory_usage = Gauge(
            'sptrans_spark_memory_usage_bytes',
            'Uso de memória do Spark',
            ['executor_id'],
            registry=self.registry
        )
        
        # === MÉTRICAS DE STORAGE ===
        self.storage_size = Gauge(
            'sptrans_storage_size_bytes',
            'Tamanho dos dados armazenados',
            ['layer', 'format'],
            registry=self.registry
        )
        
        self.storage_operations = Counter(
            'sptrans_storage_operations_total',
            'Total de operações de storage',
            ['operation', 'layer', 'status'],
            registry=self.registry
        )
        
        # === MÉTRICAS DE AIRFLOW ===
        self.dag_runs = Counter(
            'sptrans_dag_runs_total',
            'Total de execuções de DAGs',
            ['dag_id', 'status'],
            registry=self.registry
        )
        
        self.task_duration = Histogram(
            'sptrans_task_duration_seconds',
            'Duração das tasks do Airflow',
            ['dag_id', 'task_id'],
            registry=self.registry,
            buckets=(1, 10, 30, 60, 120, 300, 600)
        )
        
        self.task_failures = Counter(
            'sptrans_task_failures_total',
            'Total de falhas em tasks',
            ['dag_id', 'task_id', 'error_type'],
            registry=self.registry
        )
        
        # === MÉTRICAS DE SERVING LAYER ===
        self.serving_queries = Counter(
            'sptrans_serving_queries_total',
            'Total de queries na serving layer',
            ['table', 'status'],
            registry=self.registry
        )
        
        self.serving_query_duration = Histogram(
            'sptrans_serving_query_duration_seconds',
            'Duração das queries',
            ['table'],
            registry=self.registry,
            buckets=(0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0)
        )
        
        self.cache_hits = Counter(
            'sptrans_cache_hits_total',
            'Total de cache hits',
            ['cache_type'],
            registry=self.registry
        )
        
        self.cache_misses = Counter(
            'sptrans_cache_misses_total',
            'Total de cache misses',
            ['cache_type'],
            registry=self.registry
        )
        
        # === MÉTRICAS DE SISTEMA ===
        self.system_health = Gauge(
            'sptrans_system_health',
            'Status de saúde do sistema (1=healthy, 0=unhealthy)',
            ['component'],
            registry=self.registry
        )
        
        self.pipeline_lag = Gauge(
            'sptrans_pipeline_lag_seconds',
            'Lag do pipeline em segundos',
            ['stage'],
            registry=self.registry
        )
    
    def get_metrics(self) -> bytes:
        """
        Retorna métricas no formato Prometheus.
        
        Returns:
            Métricas serializadas
        """
        return generate_latest(self.registry)
    
    def reset_metrics(self):
        """Reseta todas as métricas (útil para testes)."""
        self.registry = CollectorRegistry()
        self._initialize_metrics()
        logger.info("Métricas resetadas")


# Instância global
metrics = MetricsCollector()


# === DECORADORES PARA INSTRUMENTAÇÃO ===

def track_api_call(endpoint: str):
    """
    Decorator para rastrear chamadas à API.
    
    Args:
        endpoint: Nome do endpoint
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            status = 'success'
            
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                status = 'error'
                error_type = type(e).__name__
                metrics.api_errors_total.labels(
                    endpoint=endpoint,
                    error_type=error_type
                ).inc()
                raise
            finally:
                duration = time.time() - start_time
                metrics.api_requests_total.labels(
                    endpoint=endpoint,
                    status=status
                ).inc()
                metrics.api_request_duration.labels(
                    endpoint=endpoint
                ).observe(duration)
        
        return wrapper
    return decorator


def track_job_execution(job_name: str):
    """
    Decorator para rastrear execução de jobs.
    
    Args:
        job_name: Nome do job
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            status = 'success'
            
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                status = 'failed'
                raise
            finally:
                duration = time.time() - start_time
                metrics.job_executions.labels(
                    job_name=job_name,
                    status=status
                ).inc()
                metrics.job_duration.labels(
                    job_name=job_name,
                    status=status
                ).observe(duration)
        
        return wrapper
    return decorator


def track_storage_operation(operation: str, layer: str):
    """
    Decorator para rastrear operações de storage.
    
    Args:
        operation: Tipo de operação (read, write, delete)
        layer: Camada do data lake (bronze, silver, gold)
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            status = 'success'
            
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                status = 'error'
                raise
            finally:
                metrics.storage_operations.labels(
                    operation=operation,
                    layer=layer,
                    status=status
                ).inc()
        
        return wrapper
    return decorator


class MetricsReporter:
    """Reporter para enviar métricas agregadas."""
    
    def __init__(self):
        self.metrics_buffer = defaultdict(list)
    
    def report_data_quality(self, layer: str, quality_metrics: Dict[str, float]):
        """
        Reporta métricas de qualidade de dados.
        
        Args:
            layer: Camada do data lake
            quality_metrics: Dict com métricas de qualidade
        """
        for metric_name, value in quality_metrics.items():
            metrics.data_quality_score.labels(
                layer=layer,
                metric=metric_name
            ).set(value)
        
        logger.info(f"Métricas de qualidade reportadas para {layer}: {quality_metrics}")
    
    def report_processing_stats(self, layer: str, source: str, 
                               total: int, invalid: int, duplicated: int):
        """
        Reporta estatísticas de processamento.
        
        Args:
            layer: Camada do data lake
            source: Fonte dos dados
            total: Total de registros processados
            invalid: Registros inválidos
            duplicated: Registros duplicados
        """
        metrics.records_processed.labels(
            layer=layer,
            source=source
        ).inc(total)
        
        if invalid > 0:
            metrics.records_invalid.labels(
                layer=layer,
                reason='validation_failed'
            ).inc(invalid)
        
        if duplicated > 0:
            metrics.records_duplicated.labels(
                layer=layer
            ).inc(duplicated)
        
        logger.info(f"Stats processamento {layer}/{source}: "
                   f"total={total}, invalid={invalid}, dup={duplicated}")
    
    def report_active_vehicles(self, vehicles_by_route: Dict[str, int]):
        """
        Reporta número de veículos ativos por rota.
        
        Args:
            vehicles_by_route: Dict com {route_code: vehicle_count}
        """
        total_routes = len(vehicles_by_route)
        metrics.active_routes.set(total_routes)
        
        for route, count in vehicles_by_route.items():
            metrics.active_vehicles.labels(route=route).set(count)
        
        logger.info(f"Veículos ativos: {sum(vehicles_by_route.values())} em {total_routes} rotas")
    
    def report_system_health(self, component: str, is_healthy: bool):
        """
        Reporta saúde de um componente do sistema.
        
        Args:
            component: Nome do componente
            is_healthy: Se está saudável
        """
        metrics.system_health.labels(component=component).set(1 if is_healthy else 0)
        
        status = "healthy" if is_healthy else "unhealthy"
        logger.info(f"Health check {component}: {status}")
    
    def report_pipeline_lag(self, stage: str, lag_seconds: float):
        """
        Reporta lag do pipeline.
        
        Args:
            stage: Estágio do pipeline
            lag_seconds: Lag em segundos
        """
        metrics.pipeline_lag.labels(stage=stage).set(lag_seconds)
        
        logger.info(f"Pipeline lag {stage}: {lag_seconds:.2f}s")


# Instância global do reporter
reporter = MetricsReporter()


class PerformanceTimer:
    """Context manager para medir performance."""
    
    def __init__(self, operation_name: str, log: bool = True):
        """
        Inicializa timer.
        
        Args:
            operation_name: Nome da operação
            log: Se deve logar o resultado
        """
        self.operation_name = operation_name
        self.log = log
        self.start_time = None
        self.duration = None
    
    def __enter__(self):
        """Inicia timer."""
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Finaliza timer e loga."""
        self.duration = time.time() - self.start_time
        
        if self.log:
            logger.info(f"{self.operation_name} completado em {self.duration:.2f}s")
        
        return False


def measure_memory_usage():
    """
    Mede uso de memória do processo atual.
    
    Returns:
        Uso de memória em bytes
    """
    try:
        import psutil
        process = psutil.Process()
        return process.memory_info().rss
    except ImportError:
        logger.warning("psutil não disponível, não é possível medir memória")
        return 0


if __name__ == "__main__":
    # Testes básicos
    print("=== Testando Métricas ===")
    
    # Testar decorator de API
    @track_api_call('posicao')
    def mock_api_call():
        time.sleep(0.1)
        return {"data": "test"}
    
    print("\n1. Testando track_api_call:")
    mock_api_call()
    print("   ✅ Métrica de API registrada")
    
    # Testar reporter
    print("\n2. Testando MetricsReporter:")
    reporter.report_data_quality('bronze', {
        'completeness': 95.5,
        'accuracy': 98.2
    })
    print("   ✅ Métricas de qualidade reportadas")
    
    # Testar timer
    print("\n3. Testando PerformanceTimer:")
    with PerformanceTimer('operacao_teste'):
        time.sleep(0.5)
    print("   ✅ Timer funcionando")
    
    # Exportar métricas
    print("\n4. Exportando métricas Prometheus:")
    output = metrics.get_metrics()
    print(f"   ✅ {len(output)} bytes de métricas exportadas")
    
    print("\n✅ Todos os testes passaram!")

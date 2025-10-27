# =============================================================================
# PROMETHEUS METRICS
# =============================================================================
# Exposição de métricas Prometheus para observabilidade
# =============================================================================

"""
Prometheus Metrics Exporter

Coleta e expõe métricas para Prometheus:
    - Counters: Contadores incrementais
    - Gauges: Valores que sobem e descem
    - Histograms: Distribuições de valores
    - Summaries: Estatísticas de valores

Métricas coletadas:
    - Pipeline: jobs executados, erros, duração
    - Dados: registros processados, qualidade
    - Infraestrutura: CPU, memória, disco
    - Negócio: veículos ativos, rotas, KPIs
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

try:
    from prometheus_client import (
        Counter,
        Gauge,
        Histogram,
        Summary,
        Info,
        start_http_server,
        CollectorRegistry,
        REGISTRY,
    )
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    Counter = Gauge = Histogram = Summary = Info = None

# =============================================================================
# LOGGING
# =============================================================================

logger = logging.getLogger(__name__)

# =============================================================================
# METRICS EXPORTER
# =============================================================================

class MetricsExporter:
    """Exportador de métricas Prometheus"""
    
    def __init__(
        self,
        port: int = 9090,
        registry: Optional[Any] = None,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Inicializa exportador de métricas.
        
        Args:
            port: Porta para expor métricas
            registry: Registry do Prometheus (default: REGISTRY)
            config: Configurações adicionais
        
        Raises:
            ImportError: Se prometheus_client não estiver instalado
        """
        if not PROMETHEUS_AVAILABLE:
            raise ImportError(
                "prometheus_client não instalado. "
                "Instale com: pip install prometheus-client"
            )
        
        self.port = port
        self.registry = registry or REGISTRY
        self.config = config or {}
        self.server_started = False
        
        # Inicializar métricas
        self._init_metrics()
        
        logger.info(f"MetricsExporter inicializado (porta: {port})")
    
    def _init_metrics(self):
        """Inicializa todas as métricas"""
        
        # ===== PIPELINE METRICS =====
        
        # Jobs executados
        self.jobs_total = Counter(
            'sptrans_jobs_total',
            'Total de jobs executados',
            ['job_name', 'status'],
            registry=self.registry
        )
        
        # Duração dos jobs
        self.job_duration = Histogram(
            'sptrans_job_duration_seconds',
            'Duração dos jobs em segundos',
            ['job_name'],
            registry=self.registry,
            buckets=(10, 30, 60, 120, 300, 600, 1800, 3600)
        )
        
        # Erros
        self.errors_total = Counter(
            'sptrans_errors_total',
            'Total de erros',
            ['error_type', 'component'],
            registry=self.registry
        )
        
        # ===== DATA METRICS =====
        
        # Registros processados
        self.records_processed = Counter(
            'sptrans_records_processed_total',
            'Total de registros processados',
            ['layer', 'source'],
            registry=self.registry
        )
        
        # Qualidade de dados
        self.data_quality_score = Gauge(
            'sptrans_data_quality_score',
            'Score de qualidade de dados (0-100)',
            ['layer', 'metric'],
            registry=self.registry
        )
        
        # ===== BUSINESS METRICS =====
        
        # Veículos ativos
        self.active_vehicles = Gauge(
            'sptrans_active_vehicles',
            'Número de veículos ativos',
            ['route_code'],
            registry=self.registry
        )
        
        # Rotas ativas
        self.active_routes = Gauge(
            'sptrans_active_routes',
            'Número de rotas ativas',
            registry=self.registry
        )
        
        # Velocidade média
        self.avg_speed = Gauge(
            'sptrans_avg_speed_kmh',
            'Velocidade média em km/h',
            ['route_code'],
            registry=self.registry
        )
        
        # Headway
        self.headway_minutes = Gauge(
            'sptrans_headway_minutes',
            'Intervalo entre ônibus em minutos',
            ['route_code'],
            registry=self.registry
        )
        
        # ===== API METRICS =====
        
        # Requisições API
        self.api_requests = Counter(
            'sptrans_api_requests_total',
            'Total de requisições à API SPTrans',
            ['endpoint', 'status'],
            registry=self.registry
        )
        
        # Latência API
        self.api_latency = Histogram(
            'sptrans_api_latency_seconds',
            'Latência de requisições à API',
            ['endpoint'],
            registry=self.registry,
            buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0)
        )
        
        # ===== INFRASTRUCTURE METRICS =====
        
        # Uso de memória
        self.memory_usage = Gauge(
            'sptrans_memory_usage_bytes',
            'Uso de memória em bytes',
            ['component'],
            registry=self.registry
        )
        
        # Conexões ativas
        self.active_connections = Gauge(
            'sptrans_active_connections',
            'Número de conexões ativas',
            ['service'],
            registry=self.registry
        )
        
        # Cache hit rate
        self.cache_hit_rate = Gauge(
            'sptrans_cache_hit_rate',
            'Taxa de acerto do cache (%)',
            ['cache_type'],
            registry=self.registry
        )
        
        # ===== SYSTEM INFO =====
        
        # Info do pipeline
        self.pipeline_info = Info(
            'sptrans_pipeline',
            'Informações do pipeline',
            registry=self.registry
        )
        
        self.pipeline_info.info({
            'version': '1.0.0',
            'environment': 'production',
        })
        
        logger.debug("Métricas inicializadas")
    
    def start(self):
        """Inicia servidor HTTP para expor métricas"""
        if self.server_started:
            logger.warning("Servidor já está rodando")
            return
        
        try:
            start_http_server(self.port, registry=self.registry)
            self.server_started = True
            logger.info(f"✓ Prometheus metrics server iniciado na porta {self.port}")
            logger.info(f"Métricas disponíveis em: http://localhost:{self.port}/metrics")
        
        except Exception as e:
            logger.error(f"Erro ao iniciar servidor de métricas: {e}")
            raise
    
    # ===== HELPER METHODS =====
    
    def increment_job_counter(self, job_name: str, status: str = "success"):
        """Incrementa contador de jobs"""
        self.jobs_total.labels(job_name=job_name, status=status).inc()
    
    def record_job_duration(self, job_name: str, duration_seconds: float):
        """Registra duração de job"""
        self.job_duration.labels(job_name=job_name).observe(duration_seconds)
    
    def increment_error_counter(self, error_type: str, component: str):
        """Incrementa contador de erros"""
        self.errors_total.labels(error_type=error_type, component=component).inc()
    
    def increment_records_processed(self, layer: str, source: str, count: int = 1):
        """Incrementa contador de registros processados"""
        self.records_processed.labels(layer=layer, source=source).inc(count)
    
    def set_data_quality_score(self, layer: str, metric: str, score: float):
        """Define score de qualidade"""
        self.data_quality_score.labels(layer=layer, metric=metric).set(score)
    
    def set_active_vehicles(self, count: int, route_code: str = "all"):
        """Define número de veículos ativos"""
        self.active_vehicles.labels(route_code=route_code).set(count)
    
    def set_active_routes(self, count: int):
        """Define número de rotas ativas"""
        self.active_routes.set(count)
    
    def set_avg_speed(self, speed_kmh: float, route_code: str = "all"):
        """Define velocidade média"""
        self.avg_speed.labels(route_code=route_code).set(speed_kmh)
    
    def set_headway(self, minutes: float, route_code: str):
        """Define headway"""
        self.headway_minutes.labels(route_code=route_code).set(minutes)
    
    def increment_api_requests(self, endpoint: str, status: str):
        """Incrementa contador de requisições API"""
        self.api_requests.labels(endpoint=endpoint, status=status).inc()
    
    def record_api_latency(self, endpoint: str, latency_seconds: float):
        """Registra latência da API"""
        self.api_latency.labels(endpoint=endpoint).observe(latency_seconds)
    
    def set_memory_usage(self, component: str, bytes_used: int):
        """Define uso de memória"""
        self.memory_usage.labels(component=component).set(bytes_used)
    
    def set_active_connections(self, service: str, count: int):
        """Define conexões ativas"""
        self.active_connections.labels(service=service).set(count)
    
    def set_cache_hit_rate(self, cache_type: str, rate: float):
        """Define taxa de acerto do cache"""
        self.cache_hit_rate.labels(cache_type=cache_type).set(rate)


# =============================================================================
# GLOBAL INSTANCE
# =============================================================================

_exporter: Optional[MetricsExporter] = None


def get_exporter() -> Optional[MetricsExporter]:
    """Retorna instância global do exporter"""
    return _exporter


def create_metrics_exporter(
    port: Optional[int] = None,
    auto_start: bool = False
) -> MetricsExporter:
    """
    Factory function para criar metrics exporter.
    
    Args:
        port: Porta (default: env PROMETHEUS_PORT ou 9090)
        auto_start: Se True, inicia servidor automaticamente
    
    Returns:
        MetricsExporter configurado
    
    Example:
        >>> exporter = create_metrics_exporter(auto_start=True)
        >>> exporter.increment_job_counter("bronze_to_silver", "success")
    """
    import os
    global _exporter
    
    port = port or int(os.getenv("PROMETHEUS_PORT", "9090"))
    
    _exporter = MetricsExporter(port=port)
    
    if auto_start:
        _exporter.start()
    
    return _exporter


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def increment_counter(metric_name: str, value: float = 1.0, labels: dict = None):
    """
    Incrementa contador genérico.
    
    Args:
        metric_name: Nome da métrica
        value: Valor a incrementar
        labels: Labels da métrica
    """
    exporter = get_exporter()
    if exporter:
        # Mapear para métrica específica
        if metric_name == "jobs_total":
            exporter.jobs_total.labels(**(labels or {})).inc(value)
        elif metric_name == "errors_total":
            exporter.errors_total.labels(**(labels or {})).inc(value)


def set_gauge(metric_name: str, value: float, labels: dict = None):
    """
    Define valor de gauge.
    
    Args:
        metric_name: Nome da métrica
        value: Valor
        labels: Labels
    """
    exporter = get_exporter()
    if exporter:
        if metric_name == "active_vehicles":
            route = (labels or {}).get("route_code", "all")
            exporter.set_active_vehicles(int(value), route)
        elif metric_name == "data_quality_score":
            layer = (labels or {}).get("layer", "unknown")
            metric = (labels or {}).get("metric", "overall")
            exporter.set_data_quality_score(layer, metric, value)


def record_histogram(metric_name: str, value: float, labels: dict = None):
    """
    Registra valor em histogram.
    
    Args:
        metric_name: Nome da métrica
        value: Valor
        labels: Labels
    """
    exporter = get_exporter()
    if exporter:
        if metric_name == "job_duration_seconds":
            job_name = (labels or {}).get("job_name", "unknown")
            exporter.record_job_duration(job_name, value)


# =============================================================================
# DECORATORS
# =============================================================================

def track_duration(metric_name: str = "operation_duration_seconds"):
    """
    Decorator para rastrear duração de função.
    
    Example:
        >>> @track_duration()
        ... def process_data():
        ...     pass
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            import time
            
            start = time.time()
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                duration = time.time() - start
                record_histogram(
                    metric_name,
                    duration,
                    labels={"function": func.__name__}
                )
        
        return wrapper
    return decorator


# =============================================================================
# MAIN (para testes)
# =============================================================================

def main():
    """Função main para testes"""
    
    logger.info("=== Testando Prometheus Metrics ===")
    
    # Criar e iniciar exporter
    exporter = create_metrics_exporter(port=9090, auto_start=True)
    
    # Simular métricas
    logger.info("\n1. Simulando métricas de jobs")
    exporter.increment_job_counter("bronze_to_silver", "success")
    exporter.record_job_duration("bronze_to_silver", 45.5)
    
    logger.info("\n2. Simulando métricas de dados")
    exporter.increment_records_processed("bronze", "api", 1000)
    exporter.set_data_quality_score("silver", "completeness", 95.5)
    
    logger.info("\n3. Simulando métricas de negócio")
    exporter.set_active_vehicles(1500)
    exporter.set_active_routes(250)
    exporter.set_avg_speed(35.2)
    
    logger.info(f"\n✓ Métricas disponíveis em: http://localhost:9090/metrics")
    logger.info("Pressione Ctrl+C para sair")
    
    try:
        import time
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("\nEncerrando...")


if __name__ == "__main__":
    main()

# =============================================================================
# END
# =============================================================================

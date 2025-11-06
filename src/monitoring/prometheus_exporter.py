"""
Prometheus Exporter Module

Responsável por exportar métricas para Prometheus:
- Métricas técnicas (latência, throughput)
- Métricas de negócio (KPIs)
- Métricas de sistema (CPU, memória)
- HTTP endpoint para scraping
"""

from typing import Dict, List, Optional
from datetime import datetime
import time

from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    Summary,
    Info,
    CollectorRegistry,
    generate_latest,
    CONTENT_TYPE_LATEST
)
from flask import Flask, Response

from src.common.logging_config import get_logger

logger = get_logger(__name__)


class PrometheusExporter:
    """
    Classe para exportar métricas para Prometheus.
    
    Gerencia diferentes tipos de métricas e expõe endpoint HTTP.
    """
    
    def __init__(
        self,
        registry: Optional[CollectorRegistry] = None,
        namespace: str = "sptrans"
    ):
        """
        Inicializa o exportador.
        
        Args:
            registry: Registry customizado (usa padrão se None)
            namespace: Namespace para métricas
        """
        self.registry = registry or CollectorRegistry()
        self.namespace = namespace
        self.logger = get_logger(self.__class__.__name__)
        
        # Inicializar métricas
        self._init_pipeline_metrics()
        self._init_data_quality_metrics()
        self._init_business_metrics()
        self._init_system_metrics()
    
    def _init_pipeline_metrics(self) -> None:
        """Inicializa métricas do pipeline."""
        
        # Contadores
        self.records_ingested = Counter(
            "records_ingested_total",
            "Total records ingested from API",
            ["source"],
            namespace=self.namespace,
            registry=self.registry
        )
        
        self.records_processed = Counter(
            "records_processed_total",
            "Total records processed through pipeline",
            ["stage", "status"],
            namespace=self.namespace,
            registry=self.registry
        )
        
        self.pipeline_errors = Counter(
            "pipeline_errors_total",
            "Total pipeline errors",
            ["stage", "error_type"],
            namespace=self.namespace,
            registry=self.registry
        )
        
        # Histogramas (latência)
        self.ingestion_duration = Histogram(
            "ingestion_duration_seconds",
            "Time spent ingesting data",
            ["source"],
            namespace=self.namespace,
            registry=self.registry,
            buckets=(0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0)
        )
        
        self.processing_duration = Histogram(
            "processing_duration_seconds",
            "Time spent processing data",
            ["stage"],
            namespace=self.namespace,
            registry=self.registry,
            buckets=(1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0)
        )
        
        # Gauges
        self.last_ingestion_timestamp = Gauge(
            "last_ingestion_timestamp",
            "Timestamp of last successful ingestion",
            ["source"],
            namespace=self.namespace,
            registry=self.registry
        )
        
        self.pipeline_lag_seconds = Gauge(
            "pipeline_lag_seconds",
            "Lag between data timestamp and processing time",
            ["stage"],
            namespace=self.namespace,
            registry=self.registry
        )
    
    def _init_data_quality_metrics(self) -> None:
        """Inicializa métricas de qualidade de dados."""
        
        self.quality_score = Gauge(
            "data_quality_score",
            "Overall data quality score (0-100)",
            ["layer"],
            namespace=self.namespace,
            registry=self.registry
        )
        
        self.invalid_records = Counter(
            "invalid_records_total",
            "Total invalid records detected",
            ["layer", "reason"],
            namespace=self.namespace,
            registry=self.registry
        )
        
        self.duplicate_records = Counter(
            "duplicate_records_total",
            "Total duplicate records detected",
            ["layer"],
            namespace=self.namespace,
            registry=self.registry
        )
        
        self.quality_alerts = Counter(
            "quality_alerts_total",
            "Total data quality alerts",
            ["severity"],
            namespace=self.namespace,
            registry=self.registry
        )
    
    def _init_business_metrics(self) -> None:
        """Inicializa métricas de negócio (KPIs)."""
        
        self.fleet_coverage = Gauge(
            "fleet_coverage_percent",
            "Percentage of active fleet",
            namespace=self.namespace,
            registry=self.registry
        )
        
        self.active_vehicles = Gauge(
            "active_vehicles",
            "Number of active vehicles",
            namespace=self.namespace,
            registry=self.registry
        )
        
        self.average_speed = Gauge(
            "average_speed_kmh",
            "Average vehicle speed in km/h",
            ["line_id"],
            namespace=self.namespace,
            registry=self.registry
        )
        
        self.headway_minutes = Gauge(
            "headway_minutes",
            "Average headway between vehicles in minutes",
            ["line_id", "stop_id"],
            namespace=self.namespace,
            registry=self.registry
        )
        
        self.on_time_performance = Gauge(
            "on_time_performance_percent",
            "Percentage of on-time arrivals",
            ["line_id"],
            namespace=self.namespace,
            registry=self.registry
        )
    
    def _init_system_metrics(self) -> None:
        """Inicializa métricas de sistema."""
        
        self.service_health = Gauge(
            "service_health",
            "Health status of services (1=healthy, 0=unhealthy)",
            ["service"],
            namespace=self.namespace,
            registry=self.registry
        )
        
        self.component_uptime_seconds = Gauge(
            "component_uptime_seconds",
            "Uptime of components in seconds",
            ["component"],
            namespace=self.namespace,
            registry=self.registry
        )
    
    # === Métodos para atualizar métricas ===
    
    def record_ingestion(
        self,
        source: str,
        record_count: int,
        duration_seconds: float
    ) -> None:
        """
        Registra ingestão de dados.
        
        Args:
            source: Fonte dos dados (api, gtfs, etc)
            record_count: Número de registros
            duration_seconds: Duração em segundos
        """
        self.records_ingested.labels(source=source).inc(record_count)
        self.ingestion_duration.labels(source=source).observe(duration_seconds)
        self.last_ingestion_timestamp.labels(source=source).set(time.time())
    
    def record_processing(
        self,
        stage: str,
        record_count: int,
        duration_seconds: float,
        status: str = "success"
    ) -> None:
        """
        Registra processamento de dados.
        
        Args:
            stage: Estágio do pipeline (bronze, silver, gold)
            record_count: Número de registros processados
            duration_seconds: Duração em segundos
            status: Status (success, failure)
        """
        self.records_processed.labels(stage=stage, status=status).inc(record_count)
        self.processing_duration.labels(stage=stage).observe(duration_seconds)
    
    def record_error(
        self,
        stage: str,
        error_type: str,
        count: int = 1
    ) -> None:
        """
        Registra erro no pipeline.
        
        Args:
            stage: Estágio onde ocorreu o erro
            error_type: Tipo de erro
            count: Número de erros
        """
        self.pipeline_errors.labels(stage=stage, error_type=error_type).inc(count)
    
    def update_quality_score(
        self,
        layer: str,
        score: float
    ) -> None:
        """
        Atualiza score de qualidade.
        
        Args:
            layer: Camada (bronze, silver, gold)
            score: Score (0-100)
        """
        self.quality_score.labels(layer=layer).set(score)
    
    def update_fleet_coverage(
        self,
        active_vehicles: int,
        total_fleet: int
    ) -> None:
        """
        Atualiza cobertura da frota.
        
        Args:
            active_vehicles: Veículos ativos
            total_fleet: Tamanho total da frota
        """
        coverage = (active_vehicles / total_fleet * 100) if total_fleet > 0 else 0
        self.fleet_coverage.set(coverage)
        self.active_vehicles.set(active_vehicles)
    
    def update_service_health(
        self,
        service: str,
        is_healthy: bool
    ) -> None:
        """
        Atualiza status de saúde de um serviço.
        
        Args:
            service: Nome do serviço
            is_healthy: Se está saudável
        """
        self.service_health.labels(service=service).set(1 if is_healthy else 0)
    
    def get_metrics(self) -> bytes:
        """
        Obtém métricas no formato Prometheus.
        
        Returns:
            Métricas em formato texto
        """
        return generate_latest(self.registry)
    
    def create_flask_app(self, port: int = 9090) -> Flask:
        """
        Cria app Flask para servir métricas.
        
        Args:
            port: Porta para o servidor
            
        Returns:
            App Flask configurado
        """
        app = Flask(__name__)
        
        @app.route("/metrics")
        def metrics():
            """Endpoint de métricas."""
            return Response(self.get_metrics(), mimetype=CONTENT_TYPE_LATEST)
        
        @app.route("/health")
        def health():
            """Health check endpoint."""
            return {"status": "healthy", "timestamp": datetime.now().isoformat()}
        
        return app
    
    def start_server(self, host: str = "0.0.0.0", port: int = 9090) -> None:
        """
        Inicia servidor HTTP para métricas.
        
        Args:
            host: Host para bind
            port: Porta
        """
        self.logger.info(f"Starting Prometheus exporter on {host}:{port}")
        
        app = self.create_flask_app(port)
        app.run(host=host, port=port, threaded=True)


def export_metrics(
    metrics: Dict[str, float],
    namespace: str = "sptrans"
) -> None:
    """
    Função utilitária para exportar métricas.
    
    Args:
        metrics: Dicionário com métricas
        namespace: Namespace para métricas
    """
    exporter = PrometheusExporter(namespace=namespace)
    
    # Mapear métricas para gauges/counters apropriados
    for metric_name, metric_value in metrics.items():
        if "quality_score" in metric_name:
            layer = metric_name.split("_")[-1] if "_" in metric_name else "unknown"
            exporter.update_quality_score(layer, metric_value)
        
        elif "fleet_coverage" in metric_name:
            exporter.fleet_coverage.set(metric_value)
        
        elif "active_vehicles" in metric_name:
            exporter.active_vehicles.set(metric_value)


# Singleton global exporter
_global_exporter: Optional[PrometheusExporter] = None


def get_global_exporter() -> PrometheusExporter:
    """Obtém ou cria o exportador global."""
    global _global_exporter
    
    if _global_exporter is None:
        _global_exporter = PrometheusExporter()
    
    return _global_exporter
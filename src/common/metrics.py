"""
Métricas Prometheus para SPTrans Pipeline.

Define e gerencia métricas para monitoramento de performance,
qualidade de dados e saúde do pipeline.
"""

import time
from contextlib import contextmanager
from functools import wraps
from typing import Any, Callable, Dict, Optional

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    Summary,
    generate_latest,
    push_to_gateway,
)

from .constants import (
    METRIC_API_ERRORS,
    METRIC_API_REQUESTS,
    METRIC_DATA_QUALITY_SCORE,
    METRIC_PIPELINE_DURATION,
    METRIC_PREFIX,
    METRIC_RECORDS_PROCESSED,
)
from .logging_config import get_logger

logger = get_logger(__name__)

# Registry global para métricas
REGISTRY = CollectorRegistry()


# =============================================================================
# API Metrics
# =============================================================================

api_requests_total = Counter(
    METRIC_API_REQUESTS,
    "Total de requisições à API SPTrans",
    ["endpoint", "status"],
    registry=REGISTRY,
)

api_errors_total = Counter(
    METRIC_API_ERRORS,
    "Total de erros na API SPTrans",
    ["endpoint", "error_type"],
    registry=REGISTRY,
)

api_request_duration = Histogram(
    f"{METRIC_PREFIX}_api_request_duration_seconds",
    "Duração das requisições à API",
    ["endpoint"],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0],
    registry=REGISTRY,
)

api_rate_limit_hits = Counter(
    f"{METRIC_PREFIX}_api_rate_limit_hits_total",
    "Número de vezes que o rate limit foi atingido",
    ["endpoint"],
    registry=REGISTRY,
)


# =============================================================================
# Pipeline Metrics
# =============================================================================

records_processed_total = Counter(
    METRIC_RECORDS_PROCESSED,
    "Total de registros processados",
    ["stage", "status"],
    registry=REGISTRY,
)

pipeline_duration_seconds = Histogram(
    METRIC_PIPELINE_DURATION,
    "Duração de execução do pipeline",
    ["stage", "job_name"],
    buckets=[1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600],
    registry=REGISTRY,
)

pipeline_runs_total = Counter(
    f"{METRIC_PREFIX}_pipeline_runs_total",
    "Total de execuções do pipeline",
    ["stage", "status"],
    registry=REGISTRY,
)

pipeline_failures_total = Counter(
    f"{METRIC_PREFIX}_pipeline_failures_total",
    "Total de falhas no pipeline",
    ["stage", "error_type"],
    registry=REGISTRY,
)

pipeline_last_success_timestamp = Gauge(
    f"{METRIC_PREFIX}_pipeline_last_success_timestamp",
    "Timestamp da última execução bem-sucedida",
    ["stage"],
    registry=REGISTRY,
)


# =============================================================================
# Data Quality Metrics
# =============================================================================

data_quality_score = Gauge(
    METRIC_DATA_QUALITY_SCORE,
    "Score de qualidade dos dados (0-1)",
    ["stage", "dimension"],
    registry=REGISTRY,
)

data_quality_checks_total = Counter(
    f"{METRIC_PREFIX}_data_quality_checks_total",
    "Total de checks de qualidade executados",
    ["stage", "check_name", "status"],
    registry=REGISTRY,
)

null_rate = Gauge(
    f"{METRIC_PREFIX}_null_rate",
    "Taxa de valores nulos",
    ["stage", "column"],
    registry=REGISTRY,
)

duplicate_rate = Gauge(
    f"{METRIC_PREFIX}_duplicate_rate",
    "Taxa de registros duplicados",
    ["stage"],
    registry=REGISTRY,
)

data_freshness_minutes = Gauge(
    f"{METRIC_PREFIX}_data_freshness_minutes",
    "Idade dos dados mais recentes em minutos",
    ["stage"],
    registry=REGISTRY,
)


# =============================================================================
# Storage Metrics
# =============================================================================

storage_operations_total = Counter(
    f"{METRIC_PREFIX}_storage_operations_total",
    "Total de operações de storage",
    ["operation", "storage_type", "status"],
    registry=REGISTRY,
)

storage_operation_duration = Histogram(
    f"{METRIC_PREFIX}_storage_operation_duration_seconds",
    "Duração das operações de storage",
    ["operation", "storage_type"],
    buckets=[0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0],
    registry=REGISTRY,
)

bytes_written = Counter(
    f"{METRIC_PREFIX}_bytes_written_total",
    "Total de bytes escritos",
    ["stage", "format"],
    registry=REGISTRY,
)

bytes_read = Counter(
    f"{METRIC_PREFIX}_bytes_read_total",
    "Total de bytes lidos",
    ["stage", "format"],
    registry=REGISTRY,
)


# =============================================================================
# Business Metrics (KPIs)
# =============================================================================

fleet_size_current = Gauge(
    f"{METRIC_PREFIX}_fleet_size_current",
    "Número atual de veículos em operação",
    registry=REGISTRY,
)

average_speed = Gauge(
    f"{METRIC_PREFIX}_average_speed_kmh",
    "Velocidade média da frota (km/h)",
    ["period"],
    registry=REGISTRY,
)

lines_operational = Gauge(
    f"{METRIC_PREFIX}_lines_operational",
    "Número de linhas em operação",
    registry=REGISTRY,
)

trips_completed = Counter(
    f"{METRIC_PREFIX}_trips_completed_total",
    "Total de viagens completadas",
    ["line_id"],
    registry=REGISTRY,
)

headway_average_minutes = Gauge(
    f"{METRIC_PREFIX}_headway_average_minutes",
    "Intervalo médio entre ônibus (headway)",
    ["line_id"],
    registry=REGISTRY,
)


# =============================================================================
# Spark Metrics
# =============================================================================

spark_job_duration = Histogram(
    f"{METRIC_PREFIX}_spark_job_duration_seconds",
    "Duração de jobs Spark",
    ["job_name", "stage"],
    buckets=[10, 30, 60, 120, 300, 600, 1200, 1800],
    registry=REGISTRY,
)

spark_records_input = Counter(
    f"{METRIC_PREFIX}_spark_records_input_total",
    "Registros de entrada em jobs Spark",
    ["job_name"],
    registry=REGISTRY,
)

spark_records_output = Counter(
    f"{METRIC_PREFIX}_spark_records_output_total",
    "Registros de saída em jobs Spark",
    ["job_name"],
    registry=REGISTRY,
)

spark_shuffle_read_bytes = Counter(
    f"{METRIC_PREFIX}_spark_shuffle_read_bytes_total",
    "Bytes lidos no shuffle",
    ["job_name"],
    registry=REGISTRY,
)


# =============================================================================
# Helper Functions & Decorators
# =============================================================================


@contextmanager
def track_duration(metric: Histogram, labels: Optional[Dict[str, str]] = None):
    """
    Context manager para rastrear duração de operações.

    Usage:
        with track_duration(pipeline_duration_seconds, {"stage": "bronze"}):
            # ... código a ser medido
    """
    start_time = time.time()
    try:
        yield
    finally:
        duration = time.time() - start_time
        if labels:
            metric.labels(**labels).observe(duration)
        else:
            metric.observe(duration)


def track_api_call(endpoint: str):
    """
    Decorator para rastrear chamadas à API.

    Usage:
        @track_api_call("positions")
        def get_positions():
            ...
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            with track_duration(
                api_request_duration, {"endpoint": endpoint}
            ):
                try:
                    result = func(*args, **kwargs)
                    api_requests_total.labels(
                        endpoint=endpoint, status="success"
                    ).inc()
                    return result
                except Exception as e:
                    api_requests_total.labels(
                        endpoint=endpoint, status="error"
                    ).inc()
                    api_errors_total.labels(
                        endpoint=endpoint, error_type=type(e).__name__
                    ).inc()
                    raise

        return wrapper

    return decorator


def track_pipeline_run(stage: str, job_name: str):
    """
    Decorator para rastrear execução de pipeline.

    Usage:
        @track_pipeline_run("bronze", "api_ingestion")
        def run_bronze_ingestion():
            ...
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            pipeline_runs_total.labels(stage=stage, status="started").inc()

            with track_duration(
                pipeline_duration_seconds, {"stage": stage, "job_name": job_name}
            ):
                try:
                    result = func(*args, **kwargs)
                    pipeline_runs_total.labels(stage=stage, status="success").inc()
                    pipeline_last_success_timestamp.labels(stage=stage).set(
                        time.time()
                    )
                    return result
                except Exception as e:
                    pipeline_runs_total.labels(stage=stage, status="failed").inc()
                    pipeline_failures_total.labels(
                        stage=stage, error_type=type(e).__name__
                    ).inc()
                    raise

        return wrapper

    return decorator


def track_spark_job(job_name: str):
    """
    Decorator para rastrear jobs Spark.

    Usage:
        @track_spark_job("bronze_to_silver")
        def transform_bronze_to_silver(df):
            ...
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            with track_duration(
                spark_job_duration, {"job_name": job_name, "stage": "processing"}
            ):
                result = func(*args, **kwargs)
                return result

        return wrapper

    return decorator


# =============================================================================
# Metric Updates
# =============================================================================


def update_dq_metrics(stage: str, dq_results: Dict[str, Any]) -> None:
    """
    Atualiza métricas de data quality baseado nos resultados.

    Args:
        stage: Estágio do pipeline
        dq_results: Dicionário com resultados de validação
    """
    if "validations" in dq_results:
        validations = dq_results["validations"]

        # Overall score
        passed_count = sum(
            1 for v in validations.values() if v.get("status") == "passed"
        )
        total_count = len(validations)
        score = passed_count / total_count if total_count > 0 else 0

        data_quality_score.labels(stage=stage, dimension="overall").set(score)

        # Individual checks
        for check_name, result in validations.items():
            status = result.get("status", "unknown")
            data_quality_checks_total.labels(
                stage=stage, check_name=check_name, status=status
            ).inc()

    logger.info(
        f"Updated DQ metrics for {stage}",
        extra={"stage": stage, "validations_count": len(dq_results.get("validations", {}))},
    )


def update_processing_metrics(
    stage: str, input_count: int, output_count: int, bytes_written_count: int
) -> None:
    """
    Atualiza métricas de processamento.

    Args:
        stage: Estágio do pipeline
        input_count: Número de registros de entrada
        output_count: Número de registros de saída
        bytes_written_count: Bytes escritos
    """
    records_processed_total.labels(stage=stage, status="input").inc(input_count)
    records_processed_total.labels(stage=stage, status="output").inc(output_count)
    bytes_written.labels(stage=stage, format="parquet").inc(bytes_written_count)

    logger.info(
        f"Updated processing metrics for {stage}",
        extra={
            "stage": stage,
            "input_count": input_count,
            "output_count": output_count,
            "bytes_written": bytes_written_count,
        },
    )


def update_business_metrics(
    fleet_size: int,
    avg_speed: float,
    lines_count: int,
    **kwargs: Any,
) -> None:
    """
    Atualiza métricas de negócio (KPIs).

    Args:
        fleet_size: Tamanho atual da frota
        avg_speed: Velocidade média (km/h)
        lines_count: Número de linhas operacionais
        **kwargs: Outras métricas opcionais
    """
    fleet_size_current.set(fleet_size)
    average_speed.labels(period="current").set(avg_speed)
    lines_operational.set(lines_count)

    logger.info(
        "Updated business metrics",
        extra={
            "fleet_size": fleet_size,
            "avg_speed": avg_speed,
            "lines_count": lines_count,
        },
    )


# =============================================================================
# Metrics Export
# =============================================================================


def get_metrics() -> bytes:
    """
    Retorna métricas em formato Prometheus.

    Returns:
        Métricas serializadas
    """
    return generate_latest(REGISTRY)


def push_metrics(gateway: str, job: str) -> None:
    """
    Push de métricas para Prometheus Pushgateway.

    Args:
        gateway: URL do Pushgateway
        job: Nome do job
    """
    try:
        push_to_gateway(gateway, job=job, registry=REGISTRY)
        logger.info(f"Metrics pushed to {gateway}", extra={"job": job})
    except Exception as e:
        logger.error(
            f"Failed to push metrics to {gateway}",
            extra={"job": job, "error": str(e)},
        )


def reset_metrics() -> None:
    """Reseta todas as métricas (útil para testes)."""
    # Nota: Nem todas as métricas podem ser resetadas
    logger.info("Metrics reset requested")


# =============================================================================
# Example Usage
# =============================================================================

if __name__ == "__main__":
    # Exemplo de uso com decorators
    @track_api_call("positions")
    def fetch_positions():
        time.sleep(0.5)  # Simula requisição
        return {"vehicles": []}

    @track_pipeline_run("bronze", "api_ingestion")
    def run_ingestion():
        time.sleep(1)
        records_processed_total.labels(stage="bronze", status="success").inc(1000)

    # Executa
    fetch_positions()
    run_ingestion()

    # Atualiza métricas de DQ
    dq_results = {
        "validations": {
            "schema": {"status": "passed"},
            "nulls": {"status": "passed"},
            "coordinates": {"status": "failed"},
        }
    }
    update_dq_metrics("bronze", dq_results)

    # Atualiza métricas de negócio
    update_business_metrics(fleet_size=12500, avg_speed=18.5, lines_count=1350)

    # Exibe métricas
    print(get_metrics().decode("utf-8"))

"""
SPTrans Pipeline - Monitoring Module

Módulo responsável por observabilidade e monitoramento:
- Health checks
- Data quality alerts  
- Prometheus metrics
- Logging e rastreamento
"""

from .health_checks import HealthChecker, check_system_health
from .data_quality_alerts import DataQualityAlerter, send_quality_alert
from .prometheus_exporter import PrometheusExporter, export_metrics

__all__ = [
    "HealthChecker",
    "check_system_health",
    "DataQualityAlerter",
    "send_quality_alert",
    "PrometheusExporter",
    "export_metrics",
]

__version__ = "2.0.0"
# =============================================================================
# SPTRANS PIPELINE - MONITORING MODULE
# =============================================================================
# Monitoramento e observabilidade do pipeline
# =============================================================================

"""
Monitoring Module

Fornece observabilidade completa do pipeline:
    - Prometheus Metrics: Métricas de performance e negócio
    - Alerts Manager: Gerenciamento de alertas
    - Health Checks: Verificação de saúde dos componentes
    - Logger: Logging estruturado e centralizado

Componentes:
    - prometheus_metrics: Coleta e exposição de métricas
    - alerts_manager: Gerenciamento e disparo de alertas
    - health_checker: Health checks dos serviços
"""

__version__ = "1.0.0"

# =============================================================================
# IMPORTS
# =============================================================================

# Prometheus Exporter
try:
    from .prometheus_exporter import (
        MetricsExporter,
        create_metrics_exporter,
        increment_counter,
        set_gauge,
        record_histogram,
    )
except ImportError:
    MetricsExporter = None
    create_metrics_exporter = None
    increment_counter = None
    set_gauge = None
    record_histogram = None

# Data Quality Alerts
try:
    from .data_quality_alerts import (
        AlertsManager,
        create_alerts_manager,
        send_alert,
        check_and_alert,
    )
except ImportError:
    AlertsManager = None
    create_alerts_manager = None
    send_alert = None
    check_and_alert = None

# Health Checks
try:
    from .health_checks import (
        HealthChecker,
        create_health_checker,
        check_system_health,
        get_component_status,
    )
except ImportError:
    HealthChecker = None
    create_health_checker = None
    check_system_health = None
    get_component_status = None

# =============================================================================
# EXPORTS
# =============================================================================

__all__ = [
    # Prometheus Exporter
    "MetricsExporter",
    "create_metrics_exporter",
    "increment_counter",
    "set_gauge",
    "record_histogram",
    
    # Data Quality Alerts
    "AlertsManager",
    "create_alerts_manager",
    "send_alert",
    "check_and_alert",
    
    # Health Checks
    "HealthChecker",
    "create_health_checker",
    "check_system_health",
    "get_component_status",
    
    # Validation
    "validate_monitoring_components",
    "get_monitoring_info",
]

# =============================================================================
# VALIDATION
# =============================================================================

def validate_monitoring_components() -> dict:
    """
    Valida disponibilidade dos componentes de monitoramento.
    
    Returns:
        Dicionário com status de cada componente
    """
    import os
    
    status = {
        "prometheus_metrics_available": MetricsExporter is not None,
        "alerts_manager_available": AlertsManager is not None,
        "health_checker_available": HealthChecker is not None,
        "prometheus_configured": bool(os.getenv("PROMETHEUS_PORT")),
    }
    
    # Verificar dependências Python
    try:
        import prometheus_client
        status["prometheus_client_installed"] = True
    except ImportError:
        status["prometheus_client_installed"] = False
    
    status["all_available"] = all([
        status["prometheus_metrics_available"],
        status["alerts_manager_available"],
        status["health_checker_available"],
    ])
    
    return status


def is_monitoring_enabled() -> bool:
    """
    Verifica se monitoramento está habilitado.
    
    Returns:
        True se monitoramento está ativo
    """
    import os
    
    enabled = os.getenv("ENABLE_MONITORING", "true").lower() == "true"
    status = validate_monitoring_components()
    
    return enabled and status["all_available"]


def get_monitoring_info() -> dict:
    """
    Retorna informações sobre o módulo de monitoramento.
    
    Returns:
        Dicionário com metadados
    """
    import os
    
    return {
        "module": "monitoring",
        "version": __version__,
        "enabled": is_monitoring_enabled(),
        "components": validate_monitoring_components(),
        "prometheus_port": os.getenv("PROMETHEUS_PORT", "9090"),
        "grafana_port": os.getenv("GRAFANA_PORT", "3000"),
    }

# =============================================================================
# INITIALIZATION
# =============================================================================

def initialize_monitoring():
    """
    Inicializa sistema de monitoramento.
    
    - Configura Prometheus metrics exporter
    - Inicializa alerts manager
    - Inicia health checks
    """
    import logging
    
    logger = logging.getLogger(__name__)
    
    if not is_monitoring_enabled():
        logger.info("Monitoramento desabilitado")
        return False
    
    logger.info("Inicializando sistema de monitoramento...")
    
    # Validar componentes
    components = validate_monitoring_components()
    
    if not components["all_available"]:
        missing = [k for k, v in components.items() if not v and k != "all_available"]
        logger.warning(f"Componentes faltando: {', '.join(missing)}")
        return False
    
    # Inicializar Prometheus exporter
    if MetricsExporter is not None:
        try:
            exporter = create_metrics_exporter()
            exporter.start()
            logger.info("✓ Prometheus metrics exporter iniciado")
        except Exception as e:
            logger.error(f"Erro ao iniciar metrics exporter: {e}")
    
    # Inicializar Alerts Manager
    if AlertsManager is not None:
        try:
            alerts = create_alerts_manager()
            logger.info("✓ Alerts manager inicializado")
        except Exception as e:
            logger.error(f"Erro ao iniciar alerts manager: {e}")
    
    # Inicializar Health Checker
    if HealthChecker is not None:
        try:
            health = create_health_checker()
            logger.info("✓ Health checker inicializado")
        except Exception as e:
            logger.error(f"Erro ao iniciar health checker: {e}")
    
    logger.info("✓ Sistema de monitoramento inicializado")
    
    return True

# =============================================================================
# QUICK ACCESS FUNCTIONS
# =============================================================================

def quick_increment_metric(metric_name: str, value: float = 1.0, labels: dict = None):
    """
    Atalho para incrementar métrica.
    
    Args:
        metric_name: Nome da métrica
        value: Valor a incrementar
        labels: Labels da métrica
    
    Example:
        >>> quick_increment_metric("api_requests_total", labels={"endpoint": "/positions"})
    """
    if increment_counter:
        increment_counter(metric_name, value, labels)


def quick_set_metric(metric_name: str, value: float, labels: dict = None):
    """
    Atalho para definir valor de métrica.
    
    Args:
        metric_name: Nome da métrica
        value: Valor
        labels: Labels
    
    Example:
        >>> quick_set_metric("active_vehicles", 1500)
    """
    if set_gauge:
        set_gauge(metric_name, value, labels)


def quick_send_alert(
    alert_type: str,
    message: str,
    severity: str = "warning"
):
    """
    Atalho para enviar alerta.
    
    Args:
        alert_type: Tipo do alerta
        message: Mensagem
        severity: Severidade (info/warning/critical)
    
    Example:
        >>> quick_send_alert("data_quality", "Quality below threshold", "warning")
    """
    if send_alert:
        send_alert(alert_type, message, severity)


def quick_health_check() -> dict:
    """
    Atalho para verificar saúde do sistema.
    
    Returns:
        Dicionário com status dos componentes
    
    Example:
        >>> health = quick_health_check()
        >>> print(health["overall_status"])
    """
    if check_system_health:
        return check_system_health()
    
    return {"status": "unknown", "error": "Health checker não disponível"}

# =============================================================================
# METRICS COLLECTION
# =============================================================================

def collect_pipeline_metrics() -> dict:
    """
    Coleta métricas gerais do pipeline.
    
    Returns:
        Dicionário com métricas
    """
    from datetime import datetime
    
    metrics = {
        "timestamp": datetime.now().isoformat(),
        "monitoring_enabled": is_monitoring_enabled(),
        "components": validate_monitoring_components(),
    }
    
    # Health status
    if check_system_health:
        try:
            metrics["health"] = check_system_health()
        except Exception as e:
            metrics["health_error"] = str(e)
    
    return metrics

# =============================================================================
# CONTEXT MANAGERS
# =============================================================================

class MonitoredOperation:
    """
    Context manager para monitorar operações.
    
    Example:
        >>> with MonitoredOperation("process_data") as monitor:
        ...     # sua operação aqui
        ...     pass
    """
    
    def __init__(self, operation_name: str):
        self.operation_name = operation_name
        self.start_time = None
    
    def __enter__(self):
        import time
        self.start_time = time.time()
        
        if increment_counter:
            increment_counter(
                "operation_started_total",
                labels={"operation": self.operation_name}
            )
        
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        import time
        duration = time.time() - self.start_time
        
        if record_histogram:
            record_histogram(
                "operation_duration_seconds",
                duration,
                labels={"operation": self.operation_name}
            )
        
        if exc_type is None:
            # Sucesso
            if increment_counter:
                increment_counter(
                    "operation_success_total",
                    labels={"operation": self.operation_name}
                )
        else:
            # Erro
            if increment_counter:
                increment_counter(
                    "operation_error_total",
                    labels={
                        "operation": self.operation_name,
                        "error_type": exc_type.__name__
                    }
                )
        
        return False  # Não suprimir exceção

# =============================================================================
# LOGGING
# =============================================================================

import logging

logger = logging.getLogger(__name__)
logger.debug(f"Monitoring module v{__version__} loaded")

# Validar componentes
components_status = validate_monitoring_components()
if not components_status["all_available"]:
    missing = [k for k, v in components_status.items() if not v and k != "all_available"]
    logger.warning(f"Componentes de monitoramento faltando: {', '.join(missing)}")
else:
    logger.info("✓ Todos os componentes de monitoramento disponíveis")

# Auto-inicializar (se habilitado e não em modo teste)
import sys
if 'pytest' not in sys.modules and is_monitoring_enabled():
    try:
        initialize_monitoring()
    except Exception as e:
        logger.warning(f"Erro na inicialização automática do monitoramento: {e}")

# =============================================================================
# END
# =============================================================================

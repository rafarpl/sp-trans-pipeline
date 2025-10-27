# =============================================================================
# ALERTS MANAGER
# =============================================================================
# Gerenciamento de alertas e notificações
# =============================================================================

"""
Alerts Manager

Gerencia alertas do pipeline:
    - Alertas de qualidade de dados
    - Alertas de falhas no pipeline
    - Alertas de performance
    - Alertas de infraestrutura

Canais de notificação:
    - Email (SMTP)
    - Slack
    - PagerDuty
    - Webhook genérico
"""

import logging
import json
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

class AlertSeverity(str, Enum):
    """Severidade de alertas"""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class AlertType(str, Enum):
    """Tipos de alertas"""
    DATA_QUALITY = "data_quality"
    PIPELINE_FAILURE = "pipeline_failure"
    PERFORMANCE = "performance"
    RESOURCE = "resource"
    SECURITY = "security"


class AlertStatus(str, Enum):
    """Status de alertas"""
    PENDING = "pending"
    SENT = "sent"
    FAILED = "failed"
    RESOLVED = "resolved"

# =============================================================================
# ALERTS MANAGER
# =============================================================================

class AlertsManager:
    """Gerenciador de alertas"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Inicializa o gerenciador de alertas.
        
        Args:
            config: Configurações (canais, thresholds, etc)
        """
        self.config = config or {}
        
        # Configurações de canais
        self.channels = self.config.get("channels", ["log"])
        
        # Thresholds
        self.thresholds = {
            "data_quality_min": self.config.get("data_quality_min", 80.0),
            "job_duration_max": self.config.get("job_duration_max", 3600),
            "error_rate_max": self.config.get("error_rate_max", 0.05),
        }
        
        # Histórico de alertas
        self.alerts_history: List[Dict[str, Any]] = []
        
        # Contadores
        self.alerts_sent = 0
        self.alerts_failed = 0
        
        logger.info(f"AlertsManager inicializado (canais: {', '.join(self.channels)})")
    
    def send_alert(
        self,
        alert_type: AlertType,
        message: str,
        severity: AlertSeverity = AlertSeverity.WARNING,
        details: Optional[Dict[str, Any]] = None,
        channels: Optional[List[str]] = None
    ) -> bool:
        """
        Envia um alerta.
        
        Args:
            alert_type: Tipo do alerta
            message: Mensagem do alerta
            severity: Severidade
            details: Detalhes adicionais
            channels: Canais específicos (None = usar padrão)
        
        Returns:
            True se enviado com sucesso
        
        Example:
            >>> manager.send_alert(
            ...     AlertType.DATA_QUALITY,
            ...     "Quality below threshold",
            ...     AlertSeverity.WARNING,
            ...     {"quality_score": 75.5}
            ... )
        """
        # Criar alerta
        alert = {
            "id": self._generate_alert_id(),
            "type": alert_type.value,
            "message": message,
            "severity": severity.value,
            "details": details or {},
            "timestamp": datetime.now().isoformat(),
            "status": AlertStatus.PENDING.value,
        }
        
        # Log do alerta
        log_level = {
            AlertSeverity.INFO: logging.INFO,
            AlertSeverity.WARNING: logging.WARNING,
            AlertSeverity.CRITICAL: logging.ERROR,
        }.get(severity, logging.WARNING)
        
        logger.log(
            log_level,
            f"[ALERT] {alert_type.value.upper()}: {message}"
        )
        
        # Canais a usar
        target_channels = channels or self.channels
        
        # Enviar para cada canal
        success = True
        
        for channel in target_channels:
            try:
                if channel == "log":
                    self._send_to_log(alert)
                
                elif channel == "email":
                    self._send_to_email(alert)
                
                elif channel == "slack":
                    self._send_to_slack(alert)
                
                elif channel == "webhook":
                    self._send_to_webhook(alert)
                
                else:
                    logger.warning(f"Canal desconhecido: {channel}")
            
            except Exception as e:
                logger.error(f"Erro ao enviar alerta para {channel}: {e}")
                success = False
        
        # Atualizar status
        if success:
            alert["status"] = AlertStatus.SENT.value
            self.alerts_sent += 1
        else:
            alert["status"] = AlertStatus.FAILED.value
            self.alerts_failed += 1
        
        # Adicionar ao histórico
        self.alerts_history.append(alert)
        
        return success
    
    def check_and_alert(
        self,
        metric_name: str,
        metric_value: float,
        threshold: float,
        comparison: str = "less_than",
        alert_type: AlertType = AlertType.PERFORMANCE,
        severity: AlertSeverity = AlertSeverity.WARNING
    ) -> bool:
        """
        Verifica métrica e envia alerta se necessário.
        
        Args:
            metric_name: Nome da métrica
            metric_value: Valor atual
            threshold: Threshold
            comparison: Tipo de comparação (less_than, greater_than, equals)
            alert_type: Tipo do alerta
            severity: Severidade
        
        Returns:
            True se alerta foi enviado
        
        Example:
            >>> manager.check_and_alert(
            ...     "data_quality_score",
            ...     75.5,
            ...     80.0,
            ...     "less_than",
            ...     AlertType.DATA_QUALITY
            ... )
        """
        should_alert = False
        
        if comparison == "less_than":
            should_alert = metric_value < threshold
        elif comparison == "greater_than":
            should_alert = metric_value > threshold
        elif comparison == "equals":
            should_alert = metric_value == threshold
        
        if should_alert:
            message = f"{metric_name} = {metric_value} ({comparison} threshold: {threshold})"
            
            return self.send_alert(
                alert_type=alert_type,
                message=message,
                severity=severity,
                details={
                    "metric_name": metric_name,
                    "metric_value": metric_value,
                    "threshold": threshold,
                    "comparison": comparison,
                }
            )
        
        return False
    
    def check_data_quality(
        self,
        quality_scores: Dict[str, float],
        layer: str = "unknown"
    ):
        """
        Verifica qualidade de dados e alerta se necessário.
        
        Args:
            quality_scores: Dicionário com scores
            layer: Camada (bronze/silver/gold)
        """
        overall_score = quality_scores.get("overall", 0)
        
        if overall_score < self.thresholds["data_quality_min"]:
            self.send_alert(
                alert_type=AlertType.DATA_QUALITY,
                message=f"Quality score baixo em {layer}: {overall_score:.1f}%",
                severity=AlertSeverity.WARNING if overall_score > 70 else AlertSeverity.CRITICAL,
                details={
                    "layer": layer,
                    "scores": quality_scores,
                    "threshold": self.thresholds["data_quality_min"],
                }
            )
    
    def check_job_duration(
        self,
        job_name: str,
        duration_seconds: float
    ):
        """
        Verifica duração de job e alerta se muito longo.
        
        Args:
            job_name: Nome do job
            duration_seconds: Duração em segundos
        """
        if duration_seconds > self.thresholds["job_duration_max"]:
            self.send_alert(
                alert_type=AlertType.PERFORMANCE,
                message=f"Job {job_name} demorou {duration_seconds:.0f}s (threshold: {self.thresholds['job_duration_max']}s)",
                severity=AlertSeverity.WARNING,
                details={
                    "job_name": job_name,
                    "duration_seconds": duration_seconds,
                    "threshold": self.thresholds["job_duration_max"],
                }
            )
    
    def _send_to_log(self, alert: Dict[str, Any]):
        """Envia alerta para log"""
        logger.info(f"Alert logged: {json.dumps(alert, indent=2)}")
    
    def _send_to_email(self, alert: Dict[str, Any]):
        """Envia alerta por email"""
        # TODO: Implementar envio de email via SMTP
        logger.info(f"[EMAIL] Sending alert: {alert['message']}")
    
    def _send_to_slack(self, alert: Dict[str, Any]):
        """Envia alerta para Slack"""
        try:
            import requests
            
            webhook_url = self.config.get("slack_webhook_url")
            
            if not webhook_url:
                logger.warning("Slack webhook URL não configurada")
                return
            
            # Formatar mensagem
            color = {
                AlertSeverity.INFO.value: "#36a64f",
                AlertSeverity.WARNING.value: "#ff9900",
                AlertSeverity.CRITICAL.value: "#ff0000",
            }.get(alert["severity"], "#cccccc")
            
            payload = {
                "attachments": [{
                    "color": color,
                    "title": f"🚨 {alert['type'].upper()}",
                    "text": alert["message"],
                    "fields": [
                        {
                            "title": "Severity",
                            "value": alert["severity"],
                            "short": True
                        },
                        {
                            "title": "Time",
                            "value": alert["timestamp"],
                            "short": True
                        }
                    ],
                    "footer": "SPTrans Pipeline",
                }]
            }
            
            response = requests.post(webhook_url, json=payload, timeout=5)
            response.raise_for_status()
            
            logger.info("✓ Alerta enviado para Slack")
        
        except Exception as e:
            logger.error(f"Erro ao enviar para Slack: {e}")
            raise
    
    def _send_to_webhook(self, alert: Dict[str, Any]):
        """Envia alerta para webhook genérico"""
        try:
            import requests
            
            webhook_url = self.config.get("webhook_url")
            
            if not webhook_url:
                logger.warning("Webhook URL não configurada")
                return
            
            response = requests.post(
                webhook_url,
                json=alert,
                timeout=5,
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()
            
            logger.info("✓ Alerta enviado para webhook")
        
        except Exception as e:
            logger.error(f"Erro ao enviar para webhook: {e}")
            raise
    
    def _generate_alert_id(self) -> str:
        """Gera ID único para alerta"""
        import uuid
        return str(uuid.uuid4())[:8]
    
    def get_alerts_history(
        self,
        limit: Optional[int] = None,
        alert_type: Optional[AlertType] = None,
        severity: Optional[AlertSeverity] = None
    ) -> List[Dict[str, Any]]:
        """
        Retorna histórico de alertas.
        
        Args:
            limit: Limitar número de resultados
            alert_type: Filtrar por tipo
            severity: Filtrar por severidade
        
        Returns:
            Lista de alertas
        """
        alerts = self.alerts_history
        
        # Filtrar
        if alert_type:
            alerts = [a for a in alerts if a["type"] == alert_type.value]
        
        if severity:
            alerts = [a for a in alerts if a["severity"] == severity.value]
        
        # Ordenar por timestamp (mais recentes primeiro)
        alerts = sorted(alerts, key=lambda x: x["timestamp"], reverse=True)
        
        # Limitar
        if limit:
            alerts = alerts[:limit]
        
        return alerts
    
    def get_stats(self) -> Dict[str, Any]:
        """Retorna estatísticas de alertas"""
        return {
            "total_alerts": len(self.alerts_history),
            "alerts_sent": self.alerts_sent,
            "alerts_failed": self.alerts_failed,
            "by_type": self._count_by_field("type"),
            "by_severity": self._count_by_field("severity"),
            "by_status": self._count_by_field("status"),
        }
    
    def _count_by_field(self, field: str) -> Dict[str, int]:
        """Conta alertas por campo"""
        counts = {}
        for alert in self.alerts_history:
            value = alert.get(field, "unknown")
            counts[value] = counts.get(value, 0) + 1
        return counts


# =============================================================================
# FACTORY FUNCTION
# =============================================================================

def create_alerts_manager(config: Optional[Dict[str, Any]] = None) -> AlertsManager:
    """
    Factory function para criar AlertsManager.
    
    Args:
        config: Configurações
    
    Returns:
        AlertsManager configurado
    
    Example:
        >>> manager = create_alerts_manager({"channels": ["log", "slack"]})
        >>> manager.send_alert(AlertType.DATA_QUALITY, "Low quality detected")
    """
    import os
    
    if config is None:
        config = {
            "channels": os.getenv("ALERT_CHANNELS", "log").split(","),
            "slack_webhook_url": os.getenv("SLACK_WEBHOOK_URL"),
            "webhook_url": os.getenv("ALERT_WEBHOOK_URL"),
        }
    
    return AlertsManager(config)


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def send_alert(
    alert_type: str,
    message: str,
    severity: str = "warning",
    details: Optional[Dict] = None
) -> bool:
    """
    Atalho para enviar alerta.
    
    Args:
        alert_type: Tipo do alerta
        message: Mensagem
        severity: Severidade
        details: Detalhes
    
    Returns:
        True se enviado
    """
    manager = create_alerts_manager()
    
    return manager.send_alert(
        alert_type=AlertType(alert_type),
        message=message,
        severity=AlertSeverity(severity),
        details=details
    )


def check_and_alert(
    metric_name: str,
    metric_value: float,
    threshold: float,
    comparison: str = "less_than"
) -> bool:
    """Atalho para verificar métrica e alertar"""
    manager = create_alerts_manager()
    
    return manager.check_and_alert(
        metric_name=metric_name,
        metric_value=metric_value,
        threshold=threshold,
        comparison=comparison
    )


# =============================================================================
# MAIN (para testes)
# =============================================================================

def main():
    """Função main para testes"""
    
    logger.info("=== Testando Alerts Manager ===")
    
    # Criar manager
    manager = create_alerts_manager({
        "channels": ["log"],
    })
    
    # Test 1: Alerta de qualidade
    logger.info("\n1. Alerta de qualidade")
    manager.send_alert(
        AlertType.DATA_QUALITY,
        "Quality score baixo: 75%",
        AlertSeverity.WARNING,
        {"score": 75.0}
    )
    
    # Test 2: Alerta de performance
    logger.info("\n2. Alerta de performance")
    manager.check_and_alert(
        "job_duration",
        4000,
        3600,
        "greater_than",
        AlertType.PERFORMANCE
    )
    
    # Test 3: Histórico
    logger.info("\n3. Histórico de alertas")
    history = manager.get_alerts_history(limit=10)
    logger.info(f"Total de alertas: {len(history)}")
    
    # Test 4: Stats
    logger.info("\n4. Estatísticas")
    stats = manager.get_stats()
    logger.info(f"Stats: {json.dumps(stats, indent=2)}")
    
    logger.info("\n✓ Testes concluídos")


if __name__ == "__main__":
    main()

# =============================================================================
# END
# =============================================================================

"""
Data Quality Alerts Module

Responsável por alertas de qualidade de dados:
- Detecção de anomalias
- Notificações (Slack, Email, etc)
- Regras de alertas
- Histórico de incidentes
"""

from typing import Dict, List, Optional
from datetime import datetime
from enum import Enum
from dataclasses import dataclass

from src.common.logging_config import get_logger
from src.common.metrics import MetricsCollector

logger = get_logger(__name__)


class AlertSeverity(Enum):
    """Severidade do alerta."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertChannel(Enum):
    """Canal de notificação."""
    LOG = "log"
    SLACK = "slack"
    EMAIL = "email"
    PAGERDUTY = "pagerduty"


@dataclass
class DataQualityAlert:
    """Representação de um alerta de qualidade de dados."""
    
    alert_id: str
    severity: AlertSeverity
    title: str
    message: str
    metric_name: str
    metric_value: float
    threshold: float
    timestamp: datetime
    details: Dict
    
    def to_dict(self) -> Dict:
        """Converte para dicionário."""
        return {
            "alert_id": self.alert_id,
            "severity": self.severity.value,
            "title": self.title,
            "message": self.message,
            "metric_name": self.metric_name,
            "metric_value": self.metric_value,
            "threshold": self.threshold,
            "timestamp": self.timestamp.isoformat(),
            "details": self.details
        }


class DataQualityAlerter:
    """
    Classe para gerenciar alertas de qualidade de dados.
    
    Monitora métricas e envia alertas quando thresholds são violados.
    """
    
    # Thresholds padrão
    DEFAULT_THRESHOLDS = {
        "quality_score_min": 95.0,          # Score mínimo de qualidade (%)
        "null_records_max": 5.0,             # % máximo de registros nulos
        "out_of_bounds_max": 1.0,            # % máximo fora dos limites
        "duplicate_records_max": 2.0,        # % máximo de duplicatas
        "invalid_speed_max": 5.0,            # % máximo de velocidade inválida
        "stale_records_max": 10.0,           # % máximo de registros antigos
        "processing_delay_max_minutes": 10,  # Delay máximo de processamento
    }
    
    def __init__(
        self,
        thresholds: Optional[Dict[str, float]] = None,
        channels: Optional[List[AlertChannel]] = None
    ):
        """
        Inicializa o alerter.
        
        Args:
            thresholds: Thresholds customizados (usa padrão se None)
            channels: Canais de notificação (LOG padrão)
        """
        self.thresholds = thresholds or self.DEFAULT_THRESHOLDS.copy()
        self.channels = channels or [AlertChannel.LOG]
        self.metrics = MetricsCollector()
        self.logger = get_logger(self.__class__.__name__)
        
        # Histórico de alertas
        self.alert_history: List[DataQualityAlert] = []
    
    def check_quality_score(
        self,
        quality_score: float,
        context: Optional[Dict] = None
    ) -> Optional[DataQualityAlert]:
        """
        Verifica se o quality score está abaixo do threshold.
        
        Args:
            quality_score: Score de qualidade (0-100)
            context: Contexto adicional
            
        Returns:
            Alert se threshold violado, None caso contrário
        """
        threshold = self.thresholds["quality_score_min"]
        
        if quality_score < threshold:
            alert = DataQualityAlert(
                alert_id=self._generate_alert_id(),
                severity=AlertSeverity.ERROR if quality_score < 90 else AlertSeverity.WARNING,
                title="Quality Score Below Threshold",
                message=f"Data quality score is {quality_score:.2f}%, below threshold of {threshold}%",
                metric_name="quality_score",
                metric_value=quality_score,
                threshold=threshold,
                timestamp=datetime.now(),
                details=context or {}
            )
            
            self._send_alert(alert)
            return alert
        
        return None
    
    def check_null_records(
        self,
        null_percent: float,
        context: Optional[Dict] = None
    ) -> Optional[DataQualityAlert]:
        """
        Verifica se % de registros nulos está acima do threshold.
        
        Args:
            null_percent: Percentual de registros nulos
            context: Contexto adicional
            
        Returns:
            Alert se threshold violado, None caso contrário
        """
        threshold = self.thresholds["null_records_max"]
        
        if null_percent > threshold:
            alert = DataQualityAlert(
                alert_id=self._generate_alert_id(),
                severity=AlertSeverity.WARNING,
                title="High Null Records Rate",
                message=f"Null records rate is {null_percent:.2f}%, above threshold of {threshold}%",
                metric_name="null_records_percent",
                metric_value=null_percent,
                threshold=threshold,
                timestamp=datetime.now(),
                details=context or {}
            )
            
            self._send_alert(alert)
            return alert
        
        return None
    
    def check_duplicates(
        self,
        duplicate_percent: float,
        context: Optional[Dict] = None
    ) -> Optional[DataQualityAlert]:
        """
        Verifica se % de duplicatas está acima do threshold.
        
        Args:
            duplicate_percent: Percentual de registros duplicados
            context: Contexto adicional
            
        Returns:
            Alert se threshold violado, None caso contrário
        """
        threshold = self.thresholds["duplicate_records_max"]
        
        if duplicate_percent > threshold:
            alert = DataQualityAlert(
                alert_id=self._generate_alert_id(),
                severity=AlertSeverity.WARNING,
                title="High Duplicate Records Rate",
                message=f"Duplicate records rate is {duplicate_percent:.2f}%, above threshold of {threshold}%",
                metric_name="duplicate_records_percent",
                metric_value=duplicate_percent,
                threshold=threshold,
                timestamp=datetime.now(),
                details=context or {}
            )
            
            self._send_alert(alert)
            return alert
        
        return None
    
    def check_processing_delay(
        self,
        delay_minutes: float,
        context: Optional[Dict] = None
    ) -> Optional[DataQualityAlert]:
        """
        Verifica se delay de processamento está acima do threshold.
        
        Args:
            delay_minutes: Delay em minutos
            context: Contexto adicional
            
        Returns:
            Alert se threshold violado, None caso contrário
        """
        threshold = self.thresholds["processing_delay_max_minutes"]
        
        if delay_minutes > threshold:
            severity = AlertSeverity.CRITICAL if delay_minutes > 30 else AlertSeverity.ERROR
            
            alert = DataQualityAlert(
                alert_id=self._generate_alert_id(),
                severity=severity,
                title="High Processing Delay",
                message=f"Processing delay is {delay_minutes:.1f} minutes, above threshold of {threshold} minutes",
                metric_name="processing_delay_minutes",
                metric_value=delay_minutes,
                threshold=threshold,
                timestamp=datetime.now(),
                details=context or {}
            )
            
            self._send_alert(alert)
            return alert
        
        return None
    
    def check_all_metrics(
        self,
        metrics: Dict[str, float],
        context: Optional[Dict] = None
    ) -> List[DataQualityAlert]:
        """
        Verifica todas as métricas de uma vez.
        
        Args:
            metrics: Dicionário com métricas a verificar
            context: Contexto adicional
            
        Returns:
            Lista de alertas gerados
        """
        alerts = []
        
        # Quality score
        if "quality_score" in metrics:
            alert = self.check_quality_score(metrics["quality_score"], context)
            if alert:
                alerts.append(alert)
        
        # Null records
        if "null_records_percent" in metrics:
            alert = self.check_null_records(metrics["null_records_percent"], context)
            if alert:
                alerts.append(alert)
        
        # Duplicates
        if "duplicate_records_percent" in metrics:
            alert = self.check_duplicates(metrics["duplicate_records_percent"], context)
            if alert:
                alerts.append(alert)
        
        # Processing delay
        if "processing_delay_minutes" in metrics:
            alert = self.check_processing_delay(metrics["processing_delay_minutes"], context)
            if alert:
                alerts.append(alert)
        
        if alerts:
            self.logger.warning(f"Generated {len(alerts)} data quality alerts")
        
        return alerts
    
    def _send_alert(self, alert: DataQualityAlert) -> None:
        """
        Envia alerta pelos canais configurados.
        
        Args:
            alert: Alert a enviar
        """
        # Adicionar ao histórico
        self.alert_history.append(alert)
        
        # Publicar métrica
        self.metrics.counter("data_quality.alerts_total", 1, {"severity": alert.severity.value})
        
        # Enviar por cada canal
        for channel in self.channels:
            try:
                if channel == AlertChannel.LOG:
                    self._send_to_log(alert)
                elif channel == AlertChannel.SLACK:
                    self._send_to_slack(alert)
                elif channel == AlertChannel.EMAIL:
                    self._send_to_email(alert)
            except Exception as e:
                self.logger.error(f"Failed to send alert via {channel.value}: {e}")
    
    def _send_to_log(self, alert: DataQualityAlert) -> None:
        """Envia alerta para o log."""
        if alert.severity == AlertSeverity.CRITICAL:
            self.logger.critical(f"[ALERT] {alert.title}: {alert.message}")
        elif alert.severity == AlertSeverity.ERROR:
            self.logger.error(f"[ALERT] {alert.title}: {alert.message}")
        elif alert.severity == AlertSeverity.WARNING:
            self.logger.warning(f"[ALERT] {alert.title}: {alert.message}")
        else:
            self.logger.info(f"[ALERT] {alert.title}: {alert.message}")
    
    def _send_to_slack(self, alert: DataQualityAlert) -> None:
        """
        Envia alerta para Slack.
        
        Args:
            alert: Alert a enviar
        """
        # TODO: Implementar integração com Slack
        self.logger.info(f"Would send to Slack: {alert.title}")
        pass
    
    def _send_to_email(self, alert: DataQualityAlert) -> None:
        """
        Envia alerta por email.
        
        Args:
            alert: Alert a enviar
        """
        # TODO: Implementar envio por email
        self.logger.info(f"Would send email: {alert.title}")
        pass
    
    def _generate_alert_id(self) -> str:
        """Gera ID único para o alerta."""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"alert-{timestamp}-{len(self.alert_history)}"
    
    def get_recent_alerts(
        self,
        count: int = 10,
        severity: Optional[AlertSeverity] = None
    ) -> List[DataQualityAlert]:
        """
        Obtém alertas recentes.
        
        Args:
            count: Número de alertas a retornar
            severity: Filtrar por severidade (opcional)
            
        Returns:
            Lista de alertas
        """
        alerts = self.alert_history
        
        if severity:
            alerts = [a for a in alerts if a.severity == severity]
        
        return alerts[-count:]


def send_quality_alert(
    title: str,
    message: str,
    severity: str = "warning",
    metric_name: str = "",
    metric_value: float = 0.0,
    threshold: float = 0.0
) -> None:
    """
    Função utilitária para enviar um alerta de qualidade.
    
    Args:
        title: Título do alerta
        message: Mensagem
        severity: Severidade (info, warning, error, critical)
        metric_name: Nome da métrica
        metric_value: Valor da métrica
        threshold: Threshold violado
    """
    alerter = DataQualityAlerter()
    
    alert = DataQualityAlert(
        alert_id=alerter._generate_alert_id(),
        severity=AlertSeverity[severity.upper()],
        title=title,
        message=message,
        metric_name=metric_name,
        metric_value=metric_value,
        threshold=threshold,
        timestamp=datetime.now(),
        details={}
    )
    
    alerter._send_alert(alert)
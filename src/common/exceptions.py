"""
Exceções Customizadas para SPTrans Pipeline.

Define hierarquia de exceções para tratamento de erros específicos
em diferentes componentes do pipeline.
"""

from typing import Any, Dict, Optional


# =============================================================================
# Base Exception
# =============================================================================


class SPTransPipelineException(Exception):
    """Exceção base para todos os erros do pipeline SPTrans."""

    def __init__(
        self,
        message: str,
        error_code: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        """
        Inicializa a exceção.

        Args:
            message: Mensagem de erro
            error_code: Código de erro (opcional)
            details: Detalhes adicionais (opcional)
        """
        self.message = message
        self.error_code = error_code or self.__class__.__name__
        self.details = details or {}
        super().__init__(self.message)

    def __str__(self) -> str:
        """Representação em string da exceção."""
        base = f"[{self.error_code}] {self.message}"
        if self.details:
            base += f" | Details: {self.details}"
        return base

    def to_dict(self) -> Dict[str, Any]:
        """Converte a exceção para dicionário."""
        return {
            "error_code": self.error_code,
            "message": self.message,
            "details": self.details,
            "exception_type": self.__class__.__name__,
        }


# =============================================================================
# Configuration Exceptions
# =============================================================================


class ConfigurationException(SPTransPipelineException):
    """Erro de configuração do sistema."""

    pass


class MissingConfigException(ConfigurationException):
    """Configuração obrigatória não encontrada."""

    def __init__(self, config_key: str):
        super().__init__(
            message=f"Configuração obrigatória não encontrada: {config_key}",
            error_code="MISSING_CONFIG",
            details={"config_key": config_key},
        )


class InvalidConfigException(ConfigurationException):
    """Configuração com valor inválido."""

    def __init__(self, config_key: str, value: Any, reason: str):
        super().__init__(
            message=f"Configuração inválida: {config_key}={value}. Razão: {reason}",
            error_code="INVALID_CONFIG",
            details={"config_key": config_key, "value": value, "reason": reason},
        )


# =============================================================================
# API Exceptions
# =============================================================================


class APIException(SPTransPipelineException):
    """Erro relacionado à API SPTrans."""

    pass


class APIAuthenticationException(APIException):
    """Erro de autenticação na API."""

    def __init__(self, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            message="Falha na autenticação com a API SPTrans",
            error_code="API_AUTH_FAILED",
            details=details,
        )


class APITimeoutException(APIException):
    """Timeout ao chamar a API."""

    def __init__(self, endpoint: str, timeout: float):
        super().__init__(
            message=f"Timeout ao chamar endpoint: {endpoint}",
            error_code="API_TIMEOUT",
            details={"endpoint": endpoint, "timeout_seconds": timeout},
        )


class APIRateLimitException(APIException):
    """Rate limit da API excedido."""

    def __init__(self, retry_after: Optional[int] = None):
        details = {"retry_after_seconds": retry_after} if retry_after else {}
        super().__init__(
            message="Rate limit da API excedido",
            error_code="API_RATE_LIMIT",
            details=details,
        )


class APIResponseException(APIException):
    """Resposta inválida ou erro da API."""

    def __init__(
        self, status_code: int, response_body: str, endpoint: Optional[str] = None
    ):
        super().__init__(
            message=f"Erro na resposta da API: HTTP {status_code}",
            error_code="API_RESPONSE_ERROR",
            details={
                "status_code": status_code,
                "response_body": response_body[:500],  # Trunca resposta longa
                "endpoint": endpoint,
            },
        )


class APIConnectionException(APIException):
    """Erro de conexão com a API."""

    def __init__(self, endpoint: str, reason: str):
        super().__init__(
            message=f"Erro de conexão com endpoint: {endpoint}",
            error_code="API_CONNECTION_ERROR",
            details={"endpoint": endpoint, "reason": reason},
        )


# =============================================================================
# Data Quality Exceptions
# =============================================================================


class DataQualityException(SPTransPipelineException):
    """Erro de qualidade de dados."""

    pass


class SchemaValidationException(DataQualityException):
    """Schema dos dados não conforme."""

    def __init__(self, expected_schema: str, actual_schema: str):
        super().__init__(
            message="Schema dos dados não conforme ao esperado",
            error_code="SCHEMA_VALIDATION_FAILED",
            details={"expected": expected_schema, "actual": actual_schema},
        )


class DataValidationException(DataQualityException):
    """Dados inválidos detectados."""

    def __init__(self, validation_rule: str, failed_records: int, total_records: int):
        super().__init__(
            message=f"Validação '{validation_rule}' falhou para {failed_records}/{total_records} registros",
            error_code="DATA_VALIDATION_FAILED",
            details={
                "validation_rule": validation_rule,
                "failed_records": failed_records,
                "total_records": total_records,
                "failure_rate": failed_records / total_records if total_records > 0 else 0,
            },
        )


class DuplicateRecordsException(DataQualityException):
    """Registros duplicados acima do threshold."""

    def __init__(self, duplicate_count: int, total_count: int, threshold: float):
        duplicate_rate = duplicate_count / total_count if total_count > 0 else 0
        super().__init__(
            message=f"Taxa de duplicados ({duplicate_rate:.2%}) excede threshold ({threshold:.2%})",
            error_code="DUPLICATE_RECORDS",
            details={
                "duplicate_count": duplicate_count,
                "total_count": total_count,
                "duplicate_rate": duplicate_rate,
                "threshold": threshold,
            },
        )


class DataFreshnessException(DataQualityException):
    """Dados não estão atualizados."""

    def __init__(self, last_update_minutes: int, threshold_minutes: int):
        super().__init__(
            message=f"Dados desatualizados: última atualização há {last_update_minutes} minutos",
            error_code="DATA_FRESHNESS_FAILED",
            details={
                "last_update_minutes": last_update_minutes,
                "threshold_minutes": threshold_minutes,
            },
        )


# =============================================================================
# Storage Exceptions
# =============================================================================


class StorageException(SPTransPipelineException):
    """Erro relacionado a storage (MinIO/S3/PostgreSQL)."""

    pass


class BucketNotFoundException(StorageException):
    """Bucket S3/MinIO não encontrado."""

    def __init__(self, bucket_name: str):
        super().__init__(
            message=f"Bucket não encontrado: {bucket_name}",
            error_code="BUCKET_NOT_FOUND",
            details={"bucket_name": bucket_name},
        )


class ObjectNotFoundException(StorageException):
    """Objeto S3/MinIO não encontrado."""

    def __init__(self, bucket: str, key: str):
        super().__init__(
            message=f"Objeto não encontrado: s3://{bucket}/{key}",
            error_code="OBJECT_NOT_FOUND",
            details={"bucket": bucket, "key": key},
        )


class StorageWriteException(StorageException):
    """Erro ao escrever no storage."""

    def __init__(self, location: str, reason: str):
        super().__init__(
            message=f"Erro ao escrever em: {location}",
            error_code="STORAGE_WRITE_ERROR",
            details={"location": location, "reason": reason},
        )


class StorageReadException(StorageException):
    """Erro ao ler do storage."""

    def __init__(self, location: str, reason: str):
        super().__init__(
            message=f"Erro ao ler de: {location}",
            error_code="STORAGE_READ_ERROR",
            details={"location": location, "reason": reason},
        )


class DatabaseConnectionException(StorageException):
    """Erro de conexão com banco de dados."""

    def __init__(self, database: str, reason: str):
        super().__init__(
            message=f"Erro ao conectar no database: {database}",
            error_code="DB_CONNECTION_ERROR",
            details={"database": database, "reason": reason},
        )


class DatabaseQueryException(StorageException):
    """Erro ao executar query no banco."""

    def __init__(self, query: str, reason: str):
        super().__init__(
            message="Erro ao executar query no database",
            error_code="DB_QUERY_ERROR",
            details={"query": query[:200], "reason": reason},  # Trunca query longa
        )


# =============================================================================
# Processing Exceptions
# =============================================================================


class ProcessingException(SPTransPipelineException):
    """Erro durante processamento de dados."""

    pass


class SparkJobException(ProcessingException):
    """Erro em job Spark."""

    def __init__(self, job_name: str, stage: str, reason: str):
        super().__init__(
            message=f"Erro no job Spark '{job_name}' no stage '{stage}'",
            error_code="SPARK_JOB_FAILED",
            details={"job_name": job_name, "stage": stage, "reason": reason},
        )


class TransformationException(ProcessingException):
    """Erro em transformação de dados."""

    def __init__(self, transformation: str, reason: str):
        super().__init__(
            message=f"Erro na transformação '{transformation}'",
            error_code="TRANSFORMATION_FAILED",
            details={"transformation": transformation, "reason": reason},
        )


class AggregationException(ProcessingException):
    """Erro em agregação de dados."""

    def __init__(self, aggregation: str, reason: str):
        super().__init__(
            message=f"Erro na agregação '{aggregation}'",
            error_code="AGGREGATION_FAILED",
            details={"aggregation": aggregation, "reason": reason},
        )


# =============================================================================
# Orchestration Exceptions
# =============================================================================


class OrchestrationException(SPTransPipelineException):
    """Erro em orquestração (Airflow)."""

    pass


class DAGExecutionException(OrchestrationException):
    """Erro na execução de DAG."""

    def __init__(self, dag_id: str, task_id: str, reason: str):
        super().__init__(
            message=f"Erro na execução da DAG '{dag_id}', task '{task_id}'",
            error_code="DAG_EXECUTION_FAILED",
            details={"dag_id": dag_id, "task_id": task_id, "reason": reason},
        )


class TaskTimeoutException(OrchestrationException):
    """Task excedeu tempo limite."""

    def __init__(self, task_id: str, timeout_seconds: int):
        super().__init__(
            message=f"Task '{task_id}' excedeu timeout de {timeout_seconds}s",
            error_code="TASK_TIMEOUT",
            details={"task_id": task_id, "timeout_seconds": timeout_seconds},
        )


class DependencyException(OrchestrationException):
    """Dependência entre tasks falhou."""

    def __init__(self, task_id: str, upstream_task: str):
        super().__init__(
            message=f"Task '{task_id}' falhou devido a dependência '{upstream_task}'",
            error_code="DEPENDENCY_FAILED",
            details={"task_id": task_id, "upstream_task": upstream_task},
        )


# =============================================================================
# Utility Functions
# =============================================================================


def handle_exception(exc: Exception, logger, context: Optional[Dict[str, Any]] = None) -> None:
    """
    Trata exceção de forma padronizada com logging.

    Args:
        exc: Exceção capturada
        logger: Logger para registrar o erro
        context: Contexto adicional (opcional)
    """
    if isinstance(exc, SPTransPipelineException):
        # Exceção customizada - log estruturado
        error_dict = exc.to_dict()
        if context:
            error_dict["context"] = context

        logger.error(
            f"Pipeline error: {exc.message}",
            extra=error_dict,
            exc_info=True,
        )
    else:
        # Exceção não tratada
        logger.error(
            f"Unhandled exception: {str(exc)}",
            extra={"exception_type": type(exc).__name__, "context": context},
            exc_info=True,
        )


def wrap_api_exception(func):
    """
    Decorator para converter exceções de requests em APIException.

    Usage:
        @wrap_api_exception
        def call_api():
            ...
    """
    from functools import wraps
    import requests

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except requests.exceptions.Timeout as e:
            raise APITimeoutException(endpoint=str(e.request.url), timeout=30)
        except requests.exceptions.ConnectionError as e:
            raise APIConnectionException(
                endpoint=str(e.request.url) if e.request else "unknown",
                reason=str(e),
            )
        except requests.exceptions.HTTPError as e:
            raise APIResponseException(
                status_code=e.response.status_code,
                response_body=e.response.text,
                endpoint=str(e.request.url),
            )
        except requests.exceptions.RequestException as e:
            raise APIException(
                message=f"Erro na requisição: {str(e)}",
                error_code="API_REQUEST_ERROR",
                details={"reason": str(e)},
            )

    return wrapper


# Exemplo de uso
if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Exemplo 1: Exceção customizada
    try:
        raise DataValidationException(
            validation_rule="latitude_range", failed_records=50, total_records=1000
        )
    except SPTransPipelineException as e:
        handle_exception(e, logger, context={"job_id": "test-123"})

    # Exemplo 2: Exceção com detalhes
    try:
        raise APIAuthenticationException(details={"token": "expired"})
    except APIException as e:
        print(f"\nException Dict: {e.to_dict()}")

    # Exemplo 3: Configuração inválida
    try:
        raise InvalidConfigException(
            config_key="API_KEY", value="", reason="Não pode ser vazio"
        )
    except ConfigurationException as e:
        print(f"\nError: {e}")

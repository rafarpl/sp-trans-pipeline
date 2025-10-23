"""
Exceções customizadas do projeto.
"""
from typing import Optional
from src.common.constants import ErrorCode


class SPTransPipelineException(Exception):
    """Exceção base para o projeto."""
    
    def __init__(self, message: str, error_code: Optional[str] = None):
        self.message = message
        self.error_code = error_code
        super().__init__(self.message)


# ============================================================================
# API EXCEPTIONS
# ============================================================================

class APIException(SPTransPipelineException):
    """Exceção base para erros de API."""
    pass


class APIAuthenticationError(APIException):
    """Erro de autenticação na API."""
    
    def __init__(self, message: str):
        super().__init__(message, ErrorCode.API_AUTHENTICATION_FAILED)


class APITimeoutError(APIException):
    """Timeout na requisição à API."""
    
    def __init__(self, message: str):
        super().__init__(message, ErrorCode.API_TIMEOUT)


class APIRateLimitError(APIException):
    """Rate limit excedido."""
    
    def __init__(self, message: str):
        super().__init__(message, ErrorCode.API_RATE_LIMIT)


class APIResponseError(APIException):
    """Resposta inválida da API."""
    
    def __init__(self, message: str):
        super().__init__(message, ErrorCode.API_INVALID_RESPONSE)


# ============================================================================
# DATA QUALITY EXCEPTIONS
# ============================================================================

class DataQualityException(SPTransPipelineException):
    """Exceção base para erros de qualidade de dados."""
    pass


class DataQualityScoreBelowThreshold(DataQualityException):
    """Score de qualidade abaixo do threshold."""
    
    def __init__(self, score: float, threshold: float):
        message = f"DQ Score ({score:.2f}) abaixo do threshold ({threshold:.2f})"
        super().__init__(message, ErrorCode.DQ_SCORE_BELOW_THRESHOLD)
        self.score = score
        self.threshold = threshold


class InvalidCoordinatesError(DataQualityException):
    """Coordenadas inválidas."""
    
    def __init__(self, lat: float, lon: float):
        message = f"Coordenadas inválidas: lat={lat}, lon={lon}"
        super().__init__(message, ErrorCode.DQ_INVALID_COORDINATES)


# ============================================================================
# STORAGE EXCEPTIONS
# ============================================================================

class StorageException(SPTransPipelineException):
    """Exceção base para erros de storage."""
    pass


class StorageConnectionError(StorageException):
    """Erro de conexão com storage."""
    
    def __init__(self, message: str):
        super().__init__(message, ErrorCode.STORAGE_CONNECTION_FAILED)


class StorageWriteError(StorageException):
    """Erro ao escrever no storage."""
    
    def __init__(self, message: str):
        super().__init__(message, ErrorCode.STORAGE_WRITE_FAILED)


class StorageReadError(StorageException):
    """Erro ao ler do storage."""
    
    def __init__(self, message: str):
        super().__init__(message, ErrorCode.STORAGE_READ_FAILED)


# ============================================================================
# SPARK EXCEPTIONS
# ============================================================================

class SparkException(SPTransPipelineException):
    """Exceção base para erros do Spark."""
    pass


class SparkJobFailedError(SparkException):
    """Spark job falhou."""
    
    def __init__(self, job_name: str, error: str):
        message = f"Spark job '{job_name}' falhou: {error}"
        super().__init__(message, ErrorCode.SPARK_JOB_FAILED)


class SparkOutOfMemoryError(SparkException):
    """Spark ficou sem memória."""
    
    def __init__(self, message: str):
        super().__init__(message, ErrorCode.SPARK_OUT_OF_MEMORY)
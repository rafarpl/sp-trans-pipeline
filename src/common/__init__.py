"""
SPTrans Pipeline - Common Module

Módulo comum contendo configurações, logging, exceções e utilitários
compartilhados por todo o pipeline.
"""

from .config import Config, get_config
from .logging_config import setup_logging, get_logger
from .exceptions import (
    SPTransPipelineException,
    ConfigurationException,
    APIException,
    DataQualityException,
    StorageException,
)

__all__ = [
    "Config",
    "get_config",
    "setup_logging",
    "get_logger",
    "SPTransPipelineException",
    "ConfigurationException",
    "APIException",
    "DataQualityException",
    "StorageException",
]

__version__ = "2.0.0"

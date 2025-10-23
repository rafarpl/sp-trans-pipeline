"""
Configuração de logging estruturado para o projeto.
Usa structlog para logs em JSON com contexto rico.
"""
import logging
import sys
from typing import Any, Dict, Optional
import structlog
from pythonjsonlogger import jsonlogger

from src.common.config import get_config
from src.common.constants import PROJECT_NAME, PROJECT_VERSION


def setup_logging() -> None:
    """
    Configura logging estruturado para todo o projeto.
    """
    config = get_config()
    log_level = getattr(logging, config.app.log_level)
    
    # Configurar logging padrão do Python
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level
    )
    
    # Configurar structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def get_logger(name: str, **context: Any) -> structlog.BoundLogger:
    """
    Retorna logger estruturado com contexto.
    
    Args:
        name: Nome do logger (geralmente __name__)
        **context: Contexto adicional para incluir em todos os logs
    
    Returns:
        Logger estruturado
    
    Example:
        >>> logger = get_logger(__name__, component="ingestion")
        >>> logger.info("processing_started", records=1000)
    """
    logger = structlog.get_logger(name)
    
    # Adicionar contexto padrão
    logger = logger.bind(
        project=PROJECT_NAME,
        version=PROJECT_VERSION,
        **context
    )
    
    return logger


class LoggerMixin:
    """
    Mixin para adicionar logging estruturado a classes.
    
    Example:
        >>> class MyClass(LoggerMixin):
        ...     def process(self):
        ...         self.logger.info("processing", step="start")
    """
    
    @property
    def logger(self) -> structlog.BoundLogger:
        """Retorna logger com contexto da classe."""
        if not hasattr(self, '_logger'):
            self._logger = get_logger(
                self.__class__.__name__,
                component=self.__class__.__module__
            )
        return self._logger


def log_function_call(logger: structlog.BoundLogger):
    """
    Decorator para logar chamadas de função.
    
    Example:
        >>> logger = get_logger(__name__)
        >>> @log_function_call(logger)
        ... def process_data(records):
        ...     pass
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            logger.debug(
                "function_called",
                function=func.__name__,
                args=str(args)[:100],
                kwargs=str(kwargs)[:100]
            )
            try:
                result = func(*args, **kwargs)
                logger.debug(
                    "function_completed",
                    function=func.__name__
                )
                return result
            except Exception as e:
                logger.error(
                    "function_failed",
                    function=func.__name__,
                    error=str(e),
                    exc_info=True
                )
                raise
        return wrapper
    return decorator


# Configurar logging na importação
setup_logging()


if __name__ == '__main__':
    # Teste de logging
    logger = get_logger(__name__, test_mode=True)
    
    logger.debug("debug message", detail="some detail")
    logger.info("info message", records_processed=1000)
    logger.warning("warning message", threshold_exceeded=True)
    logger.error("error message", error_code="E001")
    
    # Teste com contexto adicional
    request_logger = logger.bind(request_id="abc123")
    request_logger.info("request_started", method="POST")
    request_logger.info("request_completed", status_code=200)
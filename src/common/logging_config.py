"""
Configuração de Logging Estruturado para SPTrans Pipeline.

Fornece logging estruturado com suporte a JSON para produção
e formato legível para desenvolvimento.
"""

import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from pythonjsonlogger import jsonlogger


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    """Formatador JSON customizado com campos adicionais."""

    def add_fields(
        self,
        log_record: Dict[str, Any],
        record: logging.LogRecord,
        message_dict: Dict[str, Any],
    ) -> None:
        """Adiciona campos customizados ao log JSON."""
        super().add_fields(log_record, record, message_dict)

        # Timestamp ISO 8601
        log_record["timestamp"] = datetime.utcnow().isoformat() + "Z"

        # Nível do log
        log_record["level"] = record.levelname

        # Informações do código fonte
        log_record["logger"] = record.name
        log_record["module"] = record.module
        log_record["function"] = record.funcName
        log_record["line"] = record.lineno

        # Process info
        log_record["process_id"] = record.process
        log_record["thread_id"] = record.thread

        # Campos adicionais se existirem
        if hasattr(record, "job_id"):
            log_record["job_id"] = record.job_id
        if hasattr(record, "pipeline_stage"):
            log_record["pipeline_stage"] = record.pipeline_stage
        if hasattr(record, "execution_time"):
            log_record["execution_time"] = record.execution_time


class ColoredConsoleFormatter(logging.Formatter):
    """Formatador colorido para console (desenvolvimento)."""

    # Códigos ANSI para cores
    COLORS = {
        "DEBUG": "\033[36m",  # Ciano
        "INFO": "\033[32m",  # Verde
        "WARNING": "\033[33m",  # Amarelo
        "ERROR": "\033[31m",  # Vermelho
        "CRITICAL": "\033[35m",  # Magenta
    }
    RESET = "\033[0m"
    BOLD = "\033[1m"

    def format(self, record: logging.LogRecord) -> str:
        """Formata o log com cores."""
        # Cor baseada no nível
        color = self.COLORS.get(record.levelname, self.RESET)

        # Formato base
        timestamp = datetime.fromtimestamp(record.created).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        level = f"{color}{self.BOLD}{record.levelname:8s}{self.RESET}"
        logger_name = f"{color}{record.name}{self.RESET}"
        location = f"{record.filename}:{record.lineno}"

        # Mensagem
        message = record.getMessage()

        # Exception info se existir
        exc_info = ""
        if record.exc_info:
            exc_info = f"\n{self.formatException(record.exc_info)}"

        return f"{timestamp} | {level} | {logger_name:30s} | {location:30s} | {message}{exc_info}"


def setup_logging(
    log_level: str = "INFO",
    log_format: str = "json",
    log_file: Optional[Path] = None,
) -> None:
    """
    Configura o sistema de logging.

    Args:
        log_level: Nível de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Formato do log ('json' ou 'console')
        log_file: Caminho opcional para arquivo de log
    """
    # Logger raiz
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))

    # Remove handlers existentes
    root_logger.handlers.clear()

    # Handler para console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level.upper()))

    if log_format.lower() == "json":
        # Formato JSON para produção
        formatter = CustomJsonFormatter(
            "%(timestamp)s %(level)s %(name)s %(message)s"
        )
    else:
        # Formato colorido para desenvolvimento
        formatter = ColoredConsoleFormatter()

    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # Handler para arquivo (se especificado)
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(getattr(logging, log_level.upper()))

        # Sempre usar JSON para arquivo
        json_formatter = CustomJsonFormatter(
            "%(timestamp)s %(level)s %(name)s %(message)s"
        )
        file_handler.setFormatter(json_formatter)
        root_logger.addHandler(file_handler)

    # Configura loggers de bibliotecas externas
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("kafka").setLevel(logging.WARNING)
    logging.getLogger("pyspark").setLevel(logging.WARNING)
    logging.getLogger("py4j").setLevel(logging.WARNING)

    root_logger.info(
        "Logging configurado",
        extra={
            "log_level": log_level,
            "log_format": log_format,
            "log_file": str(log_file) if log_file else None,
        },
    )


def get_logger(
    name: str,
    job_id: Optional[str] = None,
    pipeline_stage: Optional[str] = None,
) -> logging.LoggerAdapter:
    """
    Retorna um logger configurado com contexto adicional.

    Args:
        name: Nome do logger (geralmente __name__)
        job_id: ID do job (opcional)
        pipeline_stage: Estágio do pipeline (bronze/silver/gold)

    Returns:
        Logger adaptado com contexto adicional
    """
    logger = logging.getLogger(name)

    # Contexto adicional
    extra = {}
    if job_id:
        extra["job_id"] = job_id
    if pipeline_stage:
        extra["pipeline_stage"] = pipeline_stage

    return logging.LoggerAdapter(logger, extra)


class LoggerContext:
    """Context manager para logging com contexto temporário."""

    def __init__(
        self,
        logger: logging.Logger,
        job_id: Optional[str] = None,
        pipeline_stage: Optional[str] = None,
        **kwargs: Any,
    ):
        """
        Inicializa o contexto.

        Args:
            logger: Logger base
            job_id: ID do job
            pipeline_stage: Estágio do pipeline
            **kwargs: Campos adicionais para contexto
        """
        self.logger = logger
        self.context = {"job_id": job_id, "pipeline_stage": pipeline_stage, **kwargs}
        self.old_context: Dict[str, Any] = {}

    def __enter__(self) -> logging.LoggerAdapter:
        """Entra no contexto."""
        # Salva contexto anterior se existir
        if hasattr(self.logger, "manager"):
            for key in self.context:
                if hasattr(self.logger, key):
                    self.old_context[key] = getattr(self.logger, key)

        return logging.LoggerAdapter(self.logger, self.context)

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Sai do contexto."""
        # Restaura contexto anterior
        for key, value in self.old_context.items():
            setattr(self.logger, key, value)


# Exemplo de uso
if __name__ == "__main__":
    # Setup para desenvolvimento
    setup_logging(log_level="DEBUG", log_format="console")

    # Logger simples
    logger = get_logger(__name__)
    logger.info("Pipeline iniciado")
    logger.debug("Modo debug ativado")
    logger.warning("Atenção: API rate limit próximo")
    logger.error("Erro ao processar dados")

    # Logger com contexto
    logger_with_context = get_logger(
        __name__, job_id="job-123", pipeline_stage="bronze"
    )
    logger_with_context.info("Processando dados da API")

    # Context manager
    base_logger = logging.getLogger(__name__)
    with LoggerContext(
        base_logger, job_id="job-456", pipeline_stage="silver"
    ) as ctx_logger:
        ctx_logger.info("Dentro do contexto temporário")

    # Simular exceção
    try:
        raise ValueError("Erro simulado")
    except ValueError:
        logger.exception("Exceção capturada")

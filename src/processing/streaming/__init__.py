# =============================================================================
# SPTRANS PIPELINE - PROCESSING STREAMING MODULE
# =============================================================================
# Processamento de dados em tempo real com Spark Structured Streaming
# =============================================================================

"""
Processing Streaming Module

Processamento de dados em tempo real (feature opcional):
    - Kafka Consumer: Consumo de eventos do Kafka
    - Real-time Processor: Processamento streaming com Spark
    - Stream to Lake: Escrita contínua no Data Lake

NOTA: Este módulo é opcional e requer:
    - Apache Kafka configurado
    - Spark com suporte a streaming
    - Conectores Kafka-Spark
"""

__version__ = "1.0.0"

# =============================================================================
# IMPORTS
# =============================================================================

try:
    from .kafka_consumer import KafkaConsumer, create_kafka_consumer
except ImportError:
    KafkaConsumer = None
    create_kafka_consumer = None

try:
    from .spark_streaming_processor import (
        SparkStreamingProcessor,
        create_streaming_session,
        process_streaming_positions,
    )
except ImportError:
    SparkStreamingProcessor = None
    create_streaming_session = None
    process_streaming_positions = None

# =============================================================================
# EXPORTS
# =============================================================================

__all__ = [
    # Kafka
    "KafkaConsumer",
    "create_kafka_consumer",
    
    # Spark Streaming
    "SparkStreamingProcessor",
    "create_streaming_session",
    "process_streaming_positions",
    
    # Validation
    "validate_streaming_support",
    "is_streaming_enabled",
]

# =============================================================================
# VALIDATION
# =============================================================================

def validate_streaming_support() -> dict:
    """
    Valida se streaming está disponível e configurado.
    
    Returns:
        Dicionário com status de suporte a streaming
    """
    import os
    
    status = {
        "kafka_consumer_available": KafkaConsumer is not None,
        "spark_streaming_available": SparkStreamingProcessor is not None,
        "kafka_bootstrap_configured": bool(os.getenv("KAFKA_BOOTSTRAP_SERVERS")),
        "streaming_enabled": os.getenv("ENABLE_STREAMING", "false").lower() == "true",
    }
    
    # Verificar dependências Python
    try:
        import kafka
        status["kafka_python_installed"] = True
    except ImportError:
        status["kafka_python_installed"] = False
    
    try:
        from pyspark.sql import SparkSession
        status["pyspark_installed"] = True
    except ImportError:
        status["pyspark_installed"] = False
    
    status["fully_supported"] = all([
        status["kafka_consumer_available"],
        status["spark_streaming_available"],
        status["kafka_python_installed"],
        status["pyspark_installed"],
    ])
    
    return status


def is_streaming_enabled() -> bool:
    """
    Verifica se streaming está habilitado.
    
    Returns:
        True se streaming está habilitado e suportado
    """
    import os
    
    enabled = os.getenv("ENABLE_STREAMING", "false").lower() == "true"
    status = validate_streaming_support()
    
    return enabled and status["fully_supported"]


def get_streaming_info() -> dict:
    """
    Retorna informações sobre configuração de streaming.
    
    Returns:
        Dicionário com informações de streaming
    """
    import os
    
    return {
        "module": "processing.streaming",
        "version": __version__,
        "enabled": is_streaming_enabled(),
        "support": validate_streaming_support(),
        "kafka_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "not configured"),
        "kafka_topic": os.getenv("KAFKA_TOPIC_POSITIONS", "sptrans-positions"),
    }

# =============================================================================
# LOGGING
# =============================================================================

import logging

logger = logging.getLogger(__name__)
logger.debug(f"Processing Streaming module v{__version__} loaded")

# Validar suporte
if is_streaming_enabled():
    logger.info("✓ Streaming está HABILITADO e suportado")
else:
    status = validate_streaming_support()
    if status["streaming_enabled"]:
        logger.warning("⚠ Streaming habilitado mas não totalmente suportado")
        missing = [k for k, v in status.items() if not v and k != "fully_supported"]
        logger.warning(f"Componentes faltando: {', '.join(missing)}")
    else:
        logger.debug("ℹ Streaming está DESABILITADO (feature opcional)")

# =============================================================================
# END
# =============================================================================

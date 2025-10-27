# =============================================================================
# KAFKA CONSUMER
# =============================================================================
# Consumidor Kafka para ingestão de dados em tempo real
# =============================================================================

"""
Kafka Consumer

Consome eventos de posições de veículos do Kafka em tempo real.

Tópicos:
    - sptrans-positions: Posições dos veículos
    - sptrans-alerts: Alertas do sistema

Funcionalidades:
    - Consumo contínuo de mensagens
    - Desserialização de JSON
    - Tratamento de erros
    - Commit de offsets
"""

import json
import logging
from typing import Optional, Dict, Any, Callable, List
from datetime import datetime

try:
    from kafka import KafkaConsumer as KafkaClient
    from kafka.errors import KafkaError
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    KafkaClient = None
    KafkaError = Exception

# =============================================================================
# LOGGING
# =============================================================================

logger = logging.getLogger(__name__)

# =============================================================================
# KAFKA CONSUMER CLASS
# =============================================================================

class KafkaConsumer:
    """Consumer Kafka para posições de veículos"""
    
    def __init__(
        self,
        bootstrap_servers: str = "kafka:9092",
        topic: str = "sptrans-positions",
        group_id: str = "sptrans-consumer-group",
        auto_offset_reset: str = "latest",
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Inicializa consumer Kafka.
        
        Args:
            bootstrap_servers: Endereços dos servidores Kafka
            topic: Tópico a consumir
            group_id: ID do consumer group
            auto_offset_reset: Estratégia de offset (earliest/latest)
            config: Configurações adicionais do Kafka
        
        Raises:
            ImportError: Se kafka-python não estiver instalado
        """
        if not KAFKA_AVAILABLE:
            raise ImportError(
                "kafka-python não instalado. "
                "Instale com: pip install kafka-python"
            )
        
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.config = config or {}
        
        # Configurações padrão
        default_config = {
            "bootstrap_servers": self.bootstrap_servers.split(","),
            "group_id": self.group_id,
            "auto_offset_reset": auto_offset_reset,
            "enable_auto_commit": True,
            "auto_commit_interval_ms": 5000,
            "value_deserializer": lambda m: json.loads(m.decode("utf-8")),
            "key_deserializer": lambda m: m.decode("utf-8") if m else None,
            "max_poll_records": 500,
            "session_timeout_ms": 30000,
            "heartbeat_interval_ms": 10000,
        }
        
        # Merge com configurações customizadas
        default_config.update(self.config)
        
        # Criar consumer
        try:
            self.consumer = KafkaClient(**default_config)
            logger.info(f"✓ Kafka Consumer criado: {topic} @ {bootstrap_servers}")
        except Exception as e:
            logger.error(f"Erro ao criar Kafka Consumer: {e}")
            raise
        
        # Estatísticas
        self.messages_consumed = 0
        self.errors = 0
    
    def subscribe(self, topics: Optional[List[str]] = None):
        """
        Inscreve em tópicos.
        
        Args:
            topics: Lista de tópicos (default: self.topic)
        """
        topics = topics or [self.topic]
        self.consumer.subscribe(topics)
        logger.info(f"Inscrito em tópicos: {topics}")
    
    def consume(
        self,
        callback: Callable[[Dict[str, Any]], None],
        max_messages: Optional[int] = None,
        timeout_ms: int = 1000
    ):
        """
        Consome mensagens e aplica callback.
        
        Args:
            callback: Função a chamar para cada mensagem
            max_messages: Máximo de mensagens (None = infinito)
            timeout_ms: Timeout para poll
        
        Example:
            >>> def process_message(msg):
            ...     print(f"Received: {msg}")
            >>> consumer.consume(process_message)
        """
        logger.info("Iniciando consumo de mensagens...")
        
        try:
            messages_processed = 0
            
            while True:
                # Poll mensagens
                msg_pack = self.consumer.poll(timeout_ms=timeout_ms)
                
                if not msg_pack:
                    continue
                
                # Processar mensagens
                for topic_partition, messages in msg_pack.items():
                    for message in messages:
                        try:
                            # Aplicar callback
                            callback(message.value)
                            
                            self.messages_consumed += 1
                            messages_processed += 1
                            
                            # Log periódico
                            if self.messages_consumed % 1000 == 0:
                                logger.info(f"Mensagens consumidas: {self.messages_consumed}")
                            
                        except Exception as e:
                            logger.error(f"Erro ao processar mensagem: {e}")
                            self.errors += 1
                
                # Verificar limite
                if max_messages and messages_processed >= max_messages:
                    logger.info(f"Limite de mensagens atingido: {max_messages}")
                    break
        
        except KeyboardInterrupt:
            logger.info("Consumo interrompido pelo usuário")
        
        except Exception as e:
            logger.error(f"Erro no consumo: {e}", exc_info=True)
            raise
        
        finally:
            logger.info(f"Total consumido: {self.messages_consumed}, Erros: {self.errors}")
    
    def consume_batch(
        self,
        batch_size: int = 100,
        timeout_ms: int = 5000
    ) -> List[Dict[str, Any]]:
        """
        Consome batch de mensagens.
        
        Args:
            batch_size: Tamanho do batch
            timeout_ms: Timeout para poll
        
        Returns:
            Lista de mensagens
        """
        messages = []
        
        try:
            msg_pack = self.consumer.poll(timeout_ms=timeout_ms, max_records=batch_size)
            
            for topic_partition, msgs in msg_pack.items():
                for message in msgs:
                    messages.append(message.value)
            
            self.messages_consumed += len(messages)
            
            logger.debug(f"Batch consumido: {len(messages)} mensagens")
        
        except Exception as e:
            logger.error(f"Erro ao consumir batch: {e}")
            self.errors += 1
        
        return messages
    
    def get_stats(self) -> Dict[str, Any]:
        """Retorna estatísticas do consumer"""
        return {
            "messages_consumed": self.messages_consumed,
            "errors": self.errors,
            "topic": self.topic,
            "group_id": self.group_id,
            "bootstrap_servers": self.bootstrap_servers,
        }
    
    def close(self):
        """Fecha consumer"""
        if self.consumer:
            logger.info("Fechando Kafka Consumer...")
            self.consumer.close()
            logger.info("✓ Consumer fechado")

    def __enter__(self):
        """Context manager entry"""
        self.subscribe()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()


# =============================================================================
# FACTORY FUNCTION
# =============================================================================

def create_kafka_consumer(
    topic: str = "sptrans-positions",
    bootstrap_servers: Optional[str] = None,
    group_id: Optional[str] = None,
    **kwargs
) -> KafkaConsumer:
    """
    Factory function para criar Kafka Consumer.
    
    Args:
        topic: Tópico a consumir
        bootstrap_servers: Servidores Kafka (default: env KAFKA_BOOTSTRAP_SERVERS)
        group_id: Consumer group ID
        **kwargs: Configurações adicionais
    
    Returns:
        KafkaConsumer configurado
    
    Example:
        >>> consumer = create_kafka_consumer(topic="sptrans-positions")
        >>> with consumer:
        ...     consumer.consume(process_message)
    """
    import os
    
    bootstrap_servers = bootstrap_servers or os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "kafka:9092"
    )
    
    group_id = group_id or os.getenv(
        "KAFKA_CONSUMER_GROUP",
        "sptrans-consumer-group"
    )
    
    return KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        group_id=group_id,
        config=kwargs
    )


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def validate_position_message(message: Dict[str, Any]) -> bool:
    """
    Valida mensagem de posição.
    
    Args:
        message: Mensagem a validar
    
    Returns:
        True se válida
    """
    required_fields = ["vehicle_id", "timestamp", "latitude", "longitude"]
    
    for field in required_fields:
        if field not in message:
            logger.warning(f"Campo obrigatório faltando: {field}")
            return False
    
    # Validar tipos
    if not isinstance(message["latitude"], (int, float)):
        return False
    
    if not isinstance(message["longitude"], (int, float)):
        return False
    
    return True


def enrich_message(message: Dict[str, Any]) -> Dict[str, Any]:
    """
    Enriquece mensagem com metadados.
    
    Args:
        message: Mensagem original
    
    Returns:
        Mensagem enriquecida
    """
    message["ingestion_timestamp"] = datetime.now().isoformat()
    message["source"] = "kafka"
    
    return message

# =============================================================================
# MAIN (para testes)
# =============================================================================

def main():
    """Função main para testes"""
    import sys
    
    def print_message(msg):
        """Callback simples"""
        print(f"[{datetime.now()}] Vehicle: {msg.get('vehicle_id')} | "
              f"Position: ({msg.get('latitude')}, {msg.get('longitude')})")
    
    try:
        consumer = create_kafka_consumer()
        
        logger.info("Iniciando consumo de teste...")
        
        with consumer:
            consumer.consume(
                callback=print_message,
                max_messages=100  # Limitar para teste
            )
        
        # Mostrar stats
        stats = consumer.get_stats()
        print(f"\n=== Estatísticas ===")
        print(f"Mensagens: {stats['messages_consumed']}")
        print(f"Erros: {stats['errors']}")
    
    except KeyboardInterrupt:
        print("\nInterrompido pelo usuário")
        sys.exit(0)
    
    except Exception as e:
        logger.error(f"Erro: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

# =============================================================================
# END
# =============================================================================

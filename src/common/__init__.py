# =============================================================================
# SPTRANS PIPELINE - COMMON MODULE
# =============================================================================
# Módulo com código compartilhado entre todos os componentes
# =============================================================================

"""
Common Module

Fornece funcionalidades compartilhadas para todo o pipeline:
    - Configurações centralizadas
    - Logging estruturado
    - Constantes do sistema
    - Exceções customizadas
    - Validadores de dados
    - Métricas e monitoramento
    - Utilitários diversos
"""

__version__ = "1.0.0"

# =============================================================================
# IMPORTS
# =============================================================================

# Config
try:
    from .config import Config, get_config
except ImportError:
    Config = None
    get_config = None

# Logging
try:
    from .logging_config import setup_logging, get_logger
except ImportError:
    setup_logging = None
    get_logger = None

# Constants
try:
    from .constants import (
        DataLakeLayer,
        SPTransEndpoint,
        GTFSFile,
        DataQualitySeverity,
        DataQualityMetric,
        ServingTable,
        JobStatus,
        ServiceQuality,
        ErrorCode,
    )
except ImportError:
    DataLakeLayer = None
    SPTransEndpoint = None
    GTFSFile = None
    DataQualitySeverity = None
    DataQualityMetric = None
    ServingTable = None
    JobStatus = None
    ServiceQuality = None
    ErrorCode = None

# Exceptions
try:
    from .exceptions import (
        SPTransException,
        APIException,
        APIConnectionError,
        APIAuthenticationError,
        APIRateLimitError,
        APIResponseError,
        DataException,
        DataValidationError,
        DataQualityError,
        SchemaValidationError,
        StorageException,
        DataLakeError,
        DatabaseError,
        ProcessingException,
        SparkJobError,
        ConfigurationError,
        GTFSException,
    )
except ImportError:
    SPTransException = Exception
    APIException = Exception
    DataException = Exception
    StorageException = Exception
    ProcessingException = Exception

# Validators
try:
    from .validators import (
        validate_coordinates,
        validate_timestamp,
        validate_vehicle_id,
        validate_route_code,
        CoordinateValidator,
        DataQualityValidator,
    )
except ImportError:
    validate_coordinates = None
    validate_timestamp = None
    validate_vehicle_id = None
    validate_route_code = None
    CoordinateValidator = None
    DataQualityValidator = None

# Metrics
try:
    from .metrics import (
        MetricsCollector,
        track_execution_time,
        increment_counter,
        set_gauge,
    )
except ImportError:
    MetricsCollector = None
    track_execution_time = None
    increment_counter = None
    set_gauge = None

# Utils
try:
    from .utils import (
        get_current_timestamp,
        parse_timestamp,
        calculate_distance,
        retry_on_failure,
        chunks,
    )
except ImportError:
    get_current_timestamp = None
    parse_timestamp = None
    calculate_distance = None
    retry_on_failure = None
    chunks = None

# =============================================================================
# EXPORTS
# =============================================================================

__all__ = [
    # Config
    "Config",
    "get_config",
    
    # Logging
    "setup_logging",
    "get_logger",
    
    # Constants - Enums
    "DataLakeLayer",
    "SPTransEndpoint",
    "GTFSFile",
    "DataQualitySeverity",
    "DataQualityMetric",
    "ServingTable",
    "JobStatus",
    "ServiceQuality",
    "ErrorCode",
    
    # Exceptions
    "SPTransException",
    "APIException",
    "APIConnectionError",
    "APIAuthenticationError",
    "APIRateLimitError",
    "APIResponseError",
    "DataException",
    "DataValidationError",
    "DataQualityError",
    "SchemaValidationError",
    "StorageException",
    "DataLakeError",
    "DatabaseError",
    "ProcessingException",
    "SparkJobError",
    "ConfigurationError",
    "GTFSException",
    
    # Validators
    "validate_coordinates",
    "validate_timestamp",
    "validate_vehicle_id",
    "validate_route_code",
    "CoordinateValidator",
    "DataQualityValidator",
    
    # Metrics
    "MetricsCollector",
    "track_execution_time",
    "increment_counter",
    "set_gauge",
    
    # Utils
    "get_current_timestamp",
    "parse_timestamp",
    "calculate_distance",
    "retry_on_failure",
    "chunks",
]

# =============================================================================
# MODULE INITIALIZATION
# =============================================================================

def initialize_common_module():
    """Inicializa o módulo common com configurações padrão"""
    
    # Setup logging se disponível
    if setup_logging:
        try:
            setup_logging()
        except Exception as e:
            import logging
            logging.warning(f"Erro ao configurar logging: {e}")
    
    # Carregar configurações se disponível
    if get_config:
        try:
            config = get_config()
            return config
        except Exception as e:
            import logging
            logging.warning(f"Erro ao carregar configurações: {e}")
    
    return None

# =============================================================================
# VALIDATION HELPERS
# =============================================================================

def validate_module_components() -> dict:
    """
    Valida quais componentes do módulo estão disponíveis.
    
    Returns:
        Dicionário com status de cada componente
    """
    components = {
        "config": Config is not None,
        "logging": setup_logging is not None,
        "constants": DataLakeLayer is not None,
        "exceptions": SPTransException is not Exception,
        "validators": validate_coordinates is not None,
        "metrics": MetricsCollector is not None,
        "utils": get_current_timestamp is not None,
    }
    
    components["all_available"] = all(components.values())
    
    return components

# =============================================================================
# QUICK ACCESS FUNCTIONS
# =============================================================================

def get_logger_quick(name: str = __name__):
    """
    Atalho para obter logger configurado.
    
    Args:
        name: Nome do logger
    
    Returns:
        Logger configurado ou logger padrão
    """
    if get_logger:
        return get_logger(name)
    
    # Fallback para logger padrão
    import logging
    return logging.getLogger(name)


def validate_data_quick(data: dict, rules: dict) -> tuple:
    """
    Atalho para validação rápida de dados.
    
    Args:
        data: Dados a validar
        rules: Regras de validação
    
    Returns:
        Tupla (is_valid, errors)
    """
    if DataQualityValidator:
        validator = DataQualityValidator()
        return validator.validate(data, rules)
    
    # Fallback básico
    return True, []


def get_config_value(key: str, default=None):
    """
    Atalho para obter valor de configuração.
    
    Args:
        key: Chave da configuração
        default: Valor padrão se não encontrado
    
    Returns:
        Valor da configuração ou default
    """
    if get_config:
        config = get_config()
        return getattr(config, key, default)
    
    # Fallback para variável de ambiente
    import os
    return os.getenv(key, default)

# =============================================================================
# CONTEXT MANAGERS
# =============================================================================

class TimedOperation:
    """Context manager para medir tempo de operação"""
    
    def __init__(self, operation_name: str):
        self.operation_name = operation_name
        self.start_time = None
        self.logger = get_logger_quick(__name__)
    
    def __enter__(self):
        import time
        self.start_time = time.time()
        self.logger.info(f"Iniciando operação: {self.operation_name}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        import time
        duration = time.time() - self.start_time
        
        if exc_type is None:
            self.logger.info(
                f"Operação '{self.operation_name}' concluída em {duration:.2f}s"
            )
        else:
            self.logger.error(
                f"Operação '{self.operation_name}' falhou após {duration:.2f}s: {exc_val}"
            )
        
        return False  # Não suprimir exceção

# =============================================================================
# MODULE INFO
# =============================================================================

def get_module_info() -> dict:
    """
    Retorna informações sobre o módulo common.
    
    Returns:
        Dicionário com metadados do módulo
    """
    return {
        "module": "common",
        "version": __version__,
        "components": validate_module_components(),
        "exports": len(__all__),
    }

# =============================================================================
# AUTO-INITIALIZATION
# =============================================================================

# Logger do módulo
logger = get_logger_quick(__name__)
logger.debug(f"Common module v{__version__} loaded")

# Validar componentes
components_status = validate_module_components()
if not components_status["all_available"]:
    missing = [k for k, v in components_status.items() if not v and k != "all_available"]
    logger.warning(f"Componentes faltando no módulo common: {', '.join(missing)}")

# Inicializar automaticamente (se não estiver em modo de teste)
import sys
if 'pytest' not in sys.modules:
    try:
        initialize_common_module()
    except Exception as e:
        logger.warning(f"Erro na inicialização do módulo common: {e}")

# =============================================================================
# END
# =============================================================================

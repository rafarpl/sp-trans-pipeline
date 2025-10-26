# =============================================================================
# SPTRANS PIPELINE - SOURCE PACKAGE
# =============================================================================
# Package principal do código fonte
# =============================================================================

"""
SPTrans Data Pipeline

Pipeline de dados em tempo real para análise do sistema de transporte público
de São Paulo, integrando dados da API Olho Vivo e arquivos GTFS.

Módulos:
    - common: Código compartilhado (config, logging, utils, validators)
    - ingestion: Camada de ingestão de dados (API, GTFS)
    - processing: Processamento com Spark (Bronze → Silver → Gold)
    - serving: Camada de serving (PostgreSQL, Redis)
    - monitoring: Monitoramento e observabilidade
"""

__version__ = "1.0.0"
__author__ = "Rafael - Data Team"
__email__ = "rafael@sptrans-pipeline.com"

# =============================================================================
# IMPORTS
# =============================================================================

# Versão do Python requerida
import sys
if sys.version_info < (3, 10):
    raise RuntimeError("SPTrans Pipeline requer Python 3.10 ou superior")

# =============================================================================
# PACKAGE METADATA
# =============================================================================

__all__ = [
    "common",
    "ingestion",
    "processing",
    "serving",
    "monitoring",
]

# =============================================================================
# CONFIGURAÇÃO INICIAL
# =============================================================================

# Configurar encoding padrão UTF-8
import locale
try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except locale.Error:
    # Fallback se locale não disponível
    pass

# Configurar timezone padrão
import os
os.environ.setdefault('TZ', 'America/Sao_Paulo')

# =============================================================================
# LOGGING SETUP
# =============================================================================

import logging

# Criar logger raiz do package
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Handler padrão se nenhum configurado
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# =============================================================================
# ENVIRONMENT VALIDATION
# =============================================================================

def validate_environment():
    """Valida se todas as dependências e variáveis de ambiente estão presentes"""
    
    required_packages = [
        'pyspark',
        'pandas',
        'psycopg2',
        'boto3',
        'requests',
    ]
    
    missing_packages = []
    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        logger.warning(
            f"Pacotes faltando: {', '.join(missing_packages)}. "
            f"Execute: pip install {' '.join(missing_packages)}"
        )
    
    # Verificar variáveis de ambiente críticas
    required_env_vars = [
        'SPTRANS_API_TOKEN',
        'POSTGRES_HOST',
        'MINIO_ENDPOINT',
    ]
    
    missing_env_vars = []
    for var in required_env_vars:
        if not os.getenv(var):
            missing_env_vars.append(var)
    
    if missing_env_vars:
        logger.warning(
            f"Variáveis de ambiente faltando: {', '.join(missing_env_vars)}"
        )

# Executar validação na importação (apenas se não for pytest)
if 'pytest' not in sys.modules:
    try:
        validate_environment()
    except Exception as e:
        logger.warning(f"Erro na validação do ambiente: {e}")

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def get_version():
    """Retorna a versão do package"""
    return __version__

def get_package_info():
    """Retorna informações sobre o package"""
    return {
        'name': 'sptrans-pipeline',
        'version': __version__,
        'author': __author__,
        'email': __email__,
        'python_version': f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
        'platform': sys.platform,
    }

# =============================================================================
# BANNER (opcional - para CLI)
# =============================================================================

def print_banner():
    """Imprime banner do projeto"""
    banner = """
    ╔═══════════════════════════════════════════════════════════════╗
    ║                                                               ║
    ║             🚌 SPTrans Data Pipeline v{version}              ║
    ║                                                               ║
    ║   Pipeline de dados em tempo real para análise do sistema    ║
    ║         de transporte público de São Paulo                   ║
    ║                                                               ║
    ╚═══════════════════════════════════════════════════════════════╝
    """.format(version=__version__)
    
    print(banner)

# =============================================================================
# INITIALIZATION MESSAGE
# =============================================================================

logger.debug(f"SPTrans Pipeline v{__version__} inicializado")

# =============================================================================
# END
# =============================================================================

"""
SPTrans Pipeline - Processing Jobs Module

Módulo contendo jobs Spark para processamento de dados
através das camadas Bronze → Silver → Gold → Serving.
"""

from .ingest_api_to_bronze import APIToBronzeJob
from .ingest_gtfs_to_bronze import GTFSToBronzeJob
from .bronze_to_silver import BronzeToSilverJob
from .silver_to_gold import SilverToGoldJob
from .gold_to_postgres import GoldToPostgresJob

__all__ = [
    "APIToBronzeJob",
    "GTFSToBronzeJob",
    "BronzeToSilverJob",
    "SilverToGoldJob",
    "GoldToPostgresJob",
]

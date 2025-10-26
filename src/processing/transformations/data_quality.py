# =============================================================================
# DATA QUALITY CHECKER
# =============================================================================
# Verificações de qualidade de dados para todas as camadas
# =============================================================================

"""
Data Quality Checker

Implementa verificações de qualidade de dados:
    - Completeness: Percentual de campos preenchidos
    - Validity: Validação de valores (coordenadas, timestamps, etc)
    - Accuracy: Precisão dos dados
    - Consistency: Consistência entre campos
    - Timeliness: Atualidade dos dados
    - Uniqueness: Verificação de duplicatas
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

# =============================================================================
# LOGGING
# =============================================================================

logger = logging.getLogger(__name__)

# =============================================================================
# DATA QUALITY CHECKER
# =============================================================================

class DataQualityChecker:
    """Verificador de qualidade de dados"""
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """
        Inicializa o checker.
        
        Args:
            spark: SparkSession
            config: Configurações opcionais
        """
        self.spark = spark
        self.config = config or {}
        
        # Thresholds padrão
        self.thresholds = {
            "completeness_min": self.config.get("completeness_min", 80.0),
            "validity_min": self.config.get("validity_min", 90.0),
            "accuracy_min": self.config.get("accuracy_min", 85.0),
            "overall_quality_min": self.config.get("overall_quality_min", 80.0),
        }
        
        # Limites geográficos de São Paulo
        self.sp_bounds = {
            "lat_min": -24.0,
            "lat_max": -23.3,
            "lon_min": -46.9,
            "lon_max": -46.3,
        }
        
        logger.info("DataQualityChecker inicializado")
    
    def check_all(self, df: DataFrame, layer: str = "unknown") -> Dict[str, Any]:
        """
        Executa todas as verificações de qualidade.
        
        Args:
            df: DataFrame a verificar
            layer: Nome da camada (bronze, silver, gold)
        
        Returns:
            Dicionário com métricas de qualidade
        """
        logger.info(f"Iniciando verificações de qualidade ({layer})")
        
        results = {
            "layer": layer,
            "timestamp": datetime.now().isoformat(),
            "total_records": df.count(),
            "metrics": {}
        }
        
        # Completeness
        results["metrics"]["completeness"] = self.check_completeness(df)
        
        # Validity
        results["metrics"]["validity"] = self.check_validity(df)
        
        # Accuracy
        results["metrics"]["accuracy"] = self.check_accuracy(df)
        
        # Consistency
        results["metrics"]["consistency"] = self.check_consistency(df)
        
        # Timeliness
        results["metrics"]["timeliness"] = self.check_timeliness(df)
        
        # Uniqueness
        results["metrics"]["uniqueness"] = self.check_uniqueness(df)
        
        # Score geral
        results["overall_quality_score"] = self._calculate_overall_score(results["metrics"])
        
        # Verificar se passou nos thresholds
        results["quality_check_passed"] = (
            results["overall_quality_score"] >= self.thresholds["overall_quality_min"]
        )
        
        logger.info(f"Quality Score: {results['overall_quality_score']:.2f}%")
        
        return results
    
    def check_completeness(self, df: DataFrame) -> Dict[str, Any]:
        """
        Verifica completude dos dados (campos não-nulos).
        
        Args:
            df: DataFrame
        
        Returns:
            Métricas de completude
        """
        logger.debug("Verificando completeness...")
        
        total_records = df.count()
        
        if total_records == 0:
            return {"score": 0.0, "details": {}}
        
        # Contar nulls por coluna
        null_counts = {}
        completeness_by_column = {}
        
        for col in df.columns:
            null_count = df.filter(F.col(col).isNull()).count()
            null_counts[col] = null_count
            completeness_pct = ((total_records - null_count) / total_records) * 100
            completeness_by_column[col] = round(completeness_pct, 2)
        
        # Score médio de completude
        avg_completeness = sum(completeness_by_column.values()) / len(completeness_by_column)
        
        return {
            "score": round(avg_completeness, 2),
            "by_column": completeness_by_column,
            "null_counts": null_counts,
            "total_records": total_records
        }
    
    def check_validity(self, df: DataFrame) -> Dict[str, Any]:
        """
        Verifica validade dos dados.
        
        Args:
            df: DataFrame
        
        Returns:
            Métricas de validade
        """
        logger.debug("Verificando validity...")
        
        total_records = df.count()
        
        if total_records == 0:
            return {"score": 0.0, "details": {}}
        
        invalid_counts = {}
        
        # Validar coordenadas (se existirem)
        if "latitude" in df.columns and "longitude" in df.columns:
            invalid_coords = df.filter(
                (F.col("latitude") < self.sp_bounds["lat_min"]) |
                (F.col("latitude") > self.sp_bounds["lat_max"]) |
                (F.col("longitude") < self.sp_bounds["lon_min"]) |
                (F.col("longitude") > self.sp_bounds["lon_max"]) |
                (F.col("latitude") == 0.0) |
                (F.col("longitude") == 0.0)
            ).count()
            
            invalid_counts["coordinates"] = invalid_coords
        
        # Validar velocidade (se existir)
        if "speed" in df.columns:
            invalid_speed = df.filter(
                (F.col("speed") < 0) | (F.col("speed") > 150)
            ).count()
            
            invalid_counts["speed"] = invalid_speed
        
        # Validar vehicle_id (se existir)
        if "vehicle_id" in df.columns:
            invalid_vehicle_id = df.filter(
                F.col("vehicle_id").isNull() |
                (F.length(F.col("vehicle_id")) < 4) |
                (F.length(F.col("vehicle_id")) > 6)
            ).count()
            
            invalid_counts["vehicle_id"] = invalid_vehicle_id
        
        # Calcular score
        total_invalid = sum(invalid_counts.values())
        validity_pct = ((total_records - total_invalid) / total_records) * 100
        
        return {
            "score": round(validity_pct, 2),
            "invalid_counts": invalid_counts,
            "total_invalid": total_invalid,
            "total_records": total_records
        }
    
    def check_accuracy(self, df: DataFrame) -> Dict[str, Any]:
        """
        Verifica precisão dos dados.
        
        Args:
            df: DataFrame
        
        Returns:
            Métricas de precisão
        """
        logger.debug("Verificando accuracy...")
        
        total_records = df.count()
        
        if total_records == 0:
            return {"score": 0.0, "details": {}}
        
        accuracy_issues = 0
        
        # Verificar coordenadas duplicadas suspeitas
        if "latitude" in df.columns and "longitude" in df.columns:
            duplicate_coords = df.groupBy("latitude", "longitude").count() \
                .filter(F.col("count") > 100).count()
            
            accuracy_issues += duplicate_coords
        
        # Verificar timestamps suspeitos
        if "timestamp" in df.columns:
            # Timestamps muito no futuro
            future_records = df.filter(
                F.col("timestamp") > F.current_timestamp()
            ).count()
            
            accuracy_issues += future_records
        
        # Score de precisão
        accuracy_pct = ((total_records - accuracy_issues) / total_records) * 100
        
        return {
            "score": round(accuracy_pct, 2),
            "accuracy_issues": accuracy_issues,
            "total_records": total_records
        }
    
    def check_consistency(self, df: DataFrame) -> Dict[str, Any]:
        """
        Verifica consistência entre campos.
        
        Args:
            df: DataFrame
        
        Returns:
            Métricas de consistência
        """
        logger.debug("Verificando consistency...")
        
        total_records = df.count()
        
        if total_records == 0:
            return {"score": 100.0, "details": {}}
        
        inconsistencies = 0
        
        # Verificar consistência entre velocidade e movimento
        if "speed" in df.columns and "is_moving" in df.columns:
            # Se speed > 0 mas is_moving = false
            inconsistent = df.filter(
                (F.col("speed") > 5) & (F.col("is_moving") == False)
            ).count()
            
            inconsistencies += inconsistent
        
        # Score de consistência
        consistency_pct = ((total_records - inconsistencies) / total_records) * 100
        
        return {
            "score": round(consistency_pct, 2),
            "inconsistencies": inconsistencies,
            "total_records": total_records
        }
    
    def check_timeliness(self, df: DataFrame) -> Dict[str, Any]:
        """
        Verifica atualidade dos dados.
        
        Args:
            df: DataFrame
        
        Returns:
            Métricas de atualidade
        """
        logger.debug("Verificando timeliness...")
        
        if "timestamp" not in df.columns:
            return {"score": 100.0, "details": "No timestamp column"}
        
        total_records = df.count()
        
        if total_records == 0:
            return {"score": 0.0, "details": {}}
        
        # Dados mais antigos que 7 dias
        seven_days_ago = datetime.now() - timedelta(days=7)
        
        old_records = df.filter(
            F.col("timestamp") < F.lit(seven_days_ago)
        ).count()
        
        # Score de atualidade
        timeliness_pct = ((total_records - old_records) / total_records) * 100
        
        # Timestamp mais recente e mais antigo
        timestamp_stats = df.agg(
            F.max("timestamp").alias("most_recent"),
            F.min("timestamp").alias("oldest")
        ).collect()[0]
        
        return {
            "score": round(timeliness_pct, 2),
            "old_records": old_records,
            "most_recent": str(timestamp_stats["most_recent"]),
            "oldest": str(timestamp_stats["oldest"]),
            "total_records": total_records
        }
    
    def check_uniqueness(self, df: DataFrame) -> Dict[str, Any]:
        """
        Verifica unicidade (duplicatas).
        
        Args:
            df: DataFrame
        
        Returns:
            Métricas de unicidade
        """
        logger.debug("Verificando uniqueness...")
        
        total_records = df.count()
        
        if total_records == 0:
            return {"score": 100.0, "details": {}}
        
        # Identificar key columns para duplicatas
        key_columns = []
        if "vehicle_id" in df.columns:
            key_columns.append("vehicle_id")
        if "timestamp" in df.columns:
            key_columns.append("timestamp")
        
        if not key_columns:
            return {"score": 100.0, "details": "No key columns to check"}
        
        # Contar registros únicos
        unique_records = df.select(key_columns).distinct().count()
        
        # Contar duplicatas
        duplicates = total_records - unique_records
        
        # Score de unicidade
        uniqueness_pct = (unique_records / total_records) * 100
        
        return {
            "score": round(uniqueness_pct, 2),
            "unique_records": unique_records,
            "duplicate_records": duplicates,
            "total_records": total_records
        }
    
    def _calculate_overall_score(self, metrics: Dict[str, Any]) -> float:
        """Calcula score geral de qualidade"""
        
        # Pesos para cada métrica
        weights = {
            "completeness": 0.25,
            "validity": 0.30,
            "accuracy": 0.20,
            "consistency": 0.10,
            "timeliness": 0.10,
            "uniqueness": 0.05,
        }
        
        weighted_sum = 0.0
        total_weight = 0.0
        
        for metric_name, weight in weights.items():
            if metric_name in metrics and "score" in metrics[metric_name]:
                weighted_sum += metrics[metric_name]["score"] * weight
                total_weight += weight
        
        if total_weight == 0:
            return 0.0
        
        return round(weighted_sum / total_weight, 2)


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def calculate_quality_score(df: DataFrame, spark: SparkSession) -> float:
    """
    Calcula score de qualidade rapidamente.
    
    Args:
        df: DataFrame
        spark: SparkSession
    
    Returns:
        Score de qualidade (0-100)
    """
    checker = DataQualityChecker(spark)
    results = checker.check_all(df)
    return results["overall_quality_score"]


def validate_positions_dataframe(df: DataFrame) -> Dict[str, Any]:
    """
    Valida DataFrame de posições.
    
    Args:
        df: DataFrame de posições
    
    Returns:
        Dicionário com resultados da validação
    """
    required_columns = ["vehicle_id", "timestamp", "latitude", "longitude"]
    
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        return {
            "is_valid": False,
            "error": f"Colunas faltando: {missing_columns}"
        }
    
    return {
        "is_valid": True,
        "total_records": df.count()
    }

# =============================================================================
# END
# =============================================================================

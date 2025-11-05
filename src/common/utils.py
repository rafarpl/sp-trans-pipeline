"""
Funções Utilitárias para SPTrans Pipeline.

Fornece funções helper para manipulação de dados, formatação,
conversões e operações comuns em todo o pipeline.
"""

import hashlib
import json
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import urlencode, urljoin

import pytz
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType

from .constants import (
    PARTITION_COLS_BRONZE,
    PARTITION_COLS_GOLD,
    PARTITION_COLS_SILVER,
    PipelineStage,
)
from .logging_config import get_logger

logger = get_logger(__name__)

# Timezone de São Paulo
SP_TZ = pytz.timezone("America/Sao_Paulo")


# =============================================================================
# Date & Time Utilities
# =============================================================================


def get_current_datetime_sp() -> datetime:
    """
    Retorna datetime atual no timezone de São Paulo.

    Returns:
        Datetime atual em SP timezone
    """
    return datetime.now(SP_TZ)


def to_utc(dt: datetime) -> datetime:
    """
    Converte datetime para UTC.

    Args:
        dt: Datetime a converter

    Returns:
        Datetime em UTC
    """
    if dt.tzinfo is None:
        dt = SP_TZ.localize(dt)
    return dt.astimezone(pytz.UTC)


def from_utc(dt: datetime) -> datetime:
    """
    Converte datetime de UTC para SP timezone.

    Args:
        dt: Datetime em UTC

    Returns:
        Datetime em SP timezone
    """
    if dt.tzinfo is None:
        dt = pytz.UTC.localize(dt)
    return dt.astimezone(SP_TZ)


def parse_datetime(dt_string: str, format: str = "%Y-%m-%d %H:%M:%S") -> datetime:
    """
    Parse string para datetime.

    Args:
        dt_string: String de datetime
        format: Formato da string

    Returns:
        Datetime parseado
    """
    return datetime.strptime(dt_string, format)


def get_date_range(
    start_date: Union[str, datetime],
    end_date: Union[str, datetime],
    date_format: str = "%Y-%m-%d",
) -> List[datetime]:
    """
    Gera lista de datas entre start e end.

    Args:
        start_date: Data inicial
        end_date: Data final
        date_format: Formato das datas se strings

    Returns:
        Lista de datetimes
    """
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, date_format)
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, date_format)

    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)

    return dates


def get_partition_path(dt: datetime, stage: PipelineStage) -> str:
    """
    Gera path de partição baseado no estágio.

    Args:
        dt: Datetime para particionamento
        stage: Estágio do pipeline

    Returns:
        String de path de partição (ex: year=2024/month=01/day=15/hour=10)
    """
    partition_cols = {
        PipelineStage.BRONZE: PARTITION_COLS_BRONZE,
        PipelineStage.SILVER: PARTITION_COLS_SILVER,
        PipelineStage.GOLD: PARTITION_COLS_GOLD,
    }.get(stage, PARTITION_COLS_SILVER)

    parts = []
    for col in partition_cols:
        if col == "year":
            parts.append(f"year={dt.year}")
        elif col == "month":
            parts.append(f"month={dt.month:02d}")
        elif col == "day":
            parts.append(f"day={dt.day:02d}")
        elif col == "hour":
            parts.append(f"hour={dt.hour:02d}")

    return "/".join(parts)


# =============================================================================
# String Utilities
# =============================================================================


def sanitize_column_name(name: str) -> str:
    """
    Sanitiza nome de coluna para uso em Spark/SQL.

    Args:
        name: Nome original

    Returns:
        Nome sanitizado (lowercase, sem espaços/caracteres especiais)
    """
    # Remove acentos
    import unicodedata

    name = "".join(
        c
        for c in unicodedata.normalize("NFKD", name)
        if not unicodedata.combining(c)
    )

    # Lowercase e substitui espaços/caracteres especiais
    name = re.sub(r"[^\w\s]", "", name.lower())
    name = re.sub(r"\s+", "_", name)

    return name


def generate_hash(data: Union[str, Dict, List]) -> str:
    """
    Gera hash MD5 de dados.

    Args:
        data: Dados para hash (string, dict ou list)

    Returns:
        Hash MD5 em hexadecimal
    """
    if isinstance(data, (dict, list)):
        data = json.dumps(data, sort_keys=True)

    return hashlib.md5(data.encode()).hexdigest()


def generate_uuid() -> str:
    """
    Gera UUID v4.

    Returns:
        UUID como string
    """
    import uuid

    return str(uuid.uuid4())


def truncate_string(text: str, max_length: int = 100, suffix: str = "...") -> str:
    """
    Trunca string se exceder max_length.

    Args:
        text: Texto a truncar
        max_length: Comprimento máximo
        suffix: Sufixo para indicar truncamento

    Returns:
        String truncada
    """
    if len(text) <= max_length:
        return text
    return text[: max_length - len(suffix)] + suffix


# =============================================================================
# DataFrame Utilities
# =============================================================================


def add_partition_columns(df: DataFrame, dt_col: str = "timestamp") -> DataFrame:
    """
    Adiciona colunas de particionamento ao DataFrame.

    Args:
        df: DataFrame Spark
        dt_col: Nome da coluna de timestamp

    Returns:
        DataFrame com colunas year, month, day, hour
    """
    return (
        df.withColumn("year", F.year(dt_col))
        .withColumn("month", F.month(dt_col))
        .withColumn("day", F.dayofmonth(dt_col))
        .withColumn("hour", F.hour(dt_col))
    )


def add_processing_metadata(df: DataFrame, stage: str, job_id: str) -> DataFrame:
    """
    Adiciona colunas de metadados de processamento.

    Args:
        df: DataFrame Spark
        stage: Estágio do pipeline
        job_id: ID do job

    Returns:
        DataFrame com colunas de metadata
    """
    return df.withColumn(
        "processed_at", F.current_timestamp()
    ).withColumn("processing_stage", F.lit(stage)).withColumn("job_id", F.lit(job_id))


def remove_duplicates(
    df: DataFrame,
    key_columns: List[str],
    order_by: Optional[str] = None,
    keep: str = "last",
) -> DataFrame:
    """
    Remove duplicatas mantendo registro mais recente/antigo.

    Args:
        df: DataFrame Spark
        key_columns: Colunas que definem duplicatas
        order_by: Coluna para ordenação (timestamp)
        keep: 'first' ou 'last'

    Returns:
        DataFrame sem duplicatas
    """
    if order_by:
        from pyspark.sql.window import Window

        if keep == "last":
            window = Window.partitionBy(key_columns).orderBy(F.desc(order_by))
        else:
            window = Window.partitionBy(key_columns).orderBy(F.asc(order_by))

        df = df.withColumn("_row_num", F.row_number().over(window))
        df = df.filter(F.col("_row_num") == 1).drop("_row_num")
    else:
        df = df.dropDuplicates(key_columns)

    return df


def fill_nulls(df: DataFrame, fill_values: Dict[str, Any]) -> DataFrame:
    """
    Preenche valores nulos com valores específicos.

    Args:
        df: DataFrame Spark
        fill_values: Dicionário {coluna: valor_default}

    Returns:
        DataFrame com nulos preenchidos
    """
    return df.fillna(fill_values)


def cast_columns(df: DataFrame, schema: Dict[str, str]) -> DataFrame:
    """
    Faz cast de colunas para tipos específicos.

    Args:
        df: DataFrame Spark
        schema: Dicionário {coluna: tipo}

    Returns:
        DataFrame com tipos convertidos
    """
    for col, dtype in schema.items():
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast(dtype))

    return df


def get_dataframe_stats(df: DataFrame) -> Dict[str, Any]:
    """
    Coleta estatísticas do DataFrame.

    Args:
        df: DataFrame Spark

    Returns:
        Dicionário com estatísticas
    """
    count = df.count()
    columns = df.columns
    dtypes = df.dtypes

    # Estimativa de tamanho (não precisa se o DataFrame for muito grande)
    size_estimate = None
    if count < 100000:  # Só estima se for pequeno
        try:
            size_estimate = df.rdd.map(lambda x: len(str(x))).sum()
        except:
            pass

    return {
        "count": count,
        "columns": columns,
        "column_count": len(columns),
        "dtypes": dict(dtypes),
        "size_bytes": size_estimate,
    }


# =============================================================================
# File & Path Utilities
# =============================================================================


def ensure_dir(path: Union[str, Path]) -> Path:
    """
    Garante que diretório existe, criando se necessário.

    Args:
        path: Caminho do diretório

    Returns:
        Path do diretório
    """
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_file_size(path: Union[str, Path]) -> int:
    """
    Retorna tamanho do arquivo em bytes.

    Args:
        path: Caminho do arquivo

    Returns:
        Tamanho em bytes
    """
    return Path(path).stat().st_size


def list_files(
    directory: Union[str, Path], pattern: str = "*", recursive: bool = False
) -> List[Path]:
    """
    Lista arquivos em diretório.

    Args:
        directory: Diretório
        pattern: Pattern de busca (glob)
        recursive: Se True, busca recursivamente

    Returns:
        Lista de Paths
    """
    directory = Path(directory)

    if recursive:
        return list(directory.rglob(pattern))
    else:
        return list(directory.glob(pattern))


def read_json_file(path: Union[str, Path]) -> Dict[str, Any]:
    """
    Lê arquivo JSON.

    Args:
        path: Caminho do arquivo

    Returns:
        Dicionário com conteúdo
    """
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json_file(data: Dict[str, Any], path: Union[str, Path]) -> None:
    """
    Escreve dados em arquivo JSON.

    Args:
        data: Dados a escrever
        path: Caminho do arquivo
    """
    ensure_dir(Path(path).parent)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


# =============================================================================
# URL & API Utilities
# =============================================================================


def build_url(base_url: str, path: str, params: Optional[Dict[str, Any]] = None) -> str:
    """
    Constrói URL com query parameters.

    Args:
        base_url: URL base
        path: Path relativo
        params: Query parameters

    Returns:
        URL completa
    """
    url = urljoin(base_url, path)

    if params:
        # Remove parâmetros None
        params = {k: v for k, v in params.items() if v is not None}
        query_string = urlencode(params)
        url = f"{url}?{query_string}"

    return url


def parse_query_params(url: str) -> Dict[str, str]:
    """
    Parse query parameters de URL.

    Args:
        url: URL completa

    Returns:
        Dicionário com parâmetros
    """
    from urllib.parse import parse_qs, urlparse

    parsed = urlparse(url)
    params = parse_qs(parsed.query)

    # Converte listas de 1 elemento em valores simples
    return {k: v[0] if len(v) == 1 else v for k, v in params.items()}


# =============================================================================
# Validation Utilities
# =============================================================================


def is_valid_coordinate(lat: float, lon: float) -> bool:
    """
    Valida se coordenadas são válidas.

    Args:
        lat: Latitude
        lon: Longitude

    Returns:
        True se válidas
    """
    return -90 <= lat <= 90 and -180 <= lon <= 180


def is_valid_email(email: str) -> bool:
    """
    Valida formato de email.

    Args:
        email: Email a validar

    Returns:
        True se válido
    """
    pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    return bool(re.match(pattern, email))


def is_valid_phone(phone: str) -> bool:
    """
    Valida formato de telefone brasileiro.

    Args:
        phone: Telefone a validar

    Returns:
        True se válido
    """
    # Remove caracteres não numéricos
    digits = re.sub(r"\D", "", phone)

    # Valida formato: (DD) 9XXXX-XXXX ou (DD) XXXX-XXXX
    return len(digits) in [10, 11] and digits[0:2].isdigit()


# =============================================================================
# Retry & Backoff
# =============================================================================


def exponential_backoff(attempt: int, base_delay: float = 1.0, max_delay: float = 60.0) -> float:
    """
    Calcula delay com backoff exponencial.

    Args:
        attempt: Número da tentativa (0-indexed)
        base_delay: Delay base em segundos
        max_delay: Delay máximo

    Returns:
        Delay em segundos
    """
    delay = min(base_delay * (2**attempt), max_delay)
    return delay


# =============================================================================
# Environment Utilities
# =============================================================================


def get_env(key: str, default: Optional[str] = None, required: bool = False) -> Optional[str]:
    """
    Obtém variável de ambiente.

    Args:
        key: Nome da variável
        default: Valor default
        required: Se True, levanta exceção se não encontrada

    Returns:
        Valor da variável

    Raises:
        ValueError: Se required=True e variável não existe
    """
    value = os.getenv(key, default)

    if required and value is None:
        raise ValueError(f"Required environment variable not set: {key}")

    return value


def is_production() -> bool:
    """
    Verifica se está em ambiente de produção.

    Returns:
        True se produção
    """
    env = get_env("ENVIRONMENT", "development").lower()
    return env in ["production", "prod"]


# Exemplo de uso
if __name__ == "__main__":
    # Date utilities
    now_sp = get_current_datetime_sp()
    print(f"Current time in SP: {now_sp}")
    print(f"Partition path: {get_partition_path(now_sp, PipelineStage.BRONZE)}")

    # String utilities
    print(f"Sanitized: {sanitize_column_name('Nome do Usuário')}")
    print(f"Hash: {generate_hash({'key': 'value'})}")
    print(f"UUID: {generate_uuid()}")

    # URL utilities
    url = build_url(
        "https://api.example.com", "/v1/users", {"id": 123, "active": True}
    )
    print(f"Built URL: {url}")

    # Validation
    print(f"Valid coord: {is_valid_coordinate(-23.5505, -46.6333)}")
    print(f"Valid email: {is_valid_email('user@example.com')}")
    print(f"Valid phone: {is_valid_phone('(11) 98765-4321')}")

    # Backoff
    for i in range(5):
        print(f"Attempt {i}: wait {exponential_backoff(i):.2f}s")

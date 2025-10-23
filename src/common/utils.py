"""
Funções utilitárias gerais do projeto.
"""
import math
from datetime import datetime, timedelta
from typing import Optional, Tuple
import pytz


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calcula distância entre dois pontos usando fórmula de Haversine.
    
    Args:
        lat1: Latitude do ponto 1 (graus)
        lon1: Longitude do ponto 1 (graus)
        lat2: Latitude do ponto 2 (graus)
        lon2: Longitude do ponto 2 (graus)
    
    Returns:
        Distância em metros
    
    Example:
        >>> dist = haversine_distance(-23.5505, -46.6333, -23.5489, -46.6388)
        >>> print(f"{dist:.2f} metros")
    """
    # Raio da Terra em metros
    R = 6371000
    
    # Converter para radianos
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    
    # Diferenças
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    
    # Fórmula de Haversine
    a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    distance = R * c
    return distance


def calculate_speed(
    lat1: float, 
    lon1: float, 
    lat2: float, 
    lon2: float,
    time1: datetime,
    time2: datetime
) -> Optional[float]:
    """
    Calcula velocidade entre dois pontos.
    
    Args:
        lat1, lon1: Coordenadas do ponto 1
        lat2, lon2: Coordenadas do ponto 2
        time1: Timestamp do ponto 1
        time2: Timestamp do ponto 2
    
    Returns:
        Velocidade em km/h, ou None se tempo for zero
    
    Example:
        >>> t1 = datetime(2025, 10, 20, 14, 0, 0)
        >>> t2 = datetime(2025, 10, 20, 14, 5, 0)
        >>> speed = calculate_speed(-23.5505, -46.6333, -23.5489, -46.6388, t1, t2)
    """
    # Calcular distância em metros
    distance_m = haversine_distance(lat1, lon1, lat2, lon2)
    
    # Calcular tempo em segundos
    time_diff_s = (time2 - time1).total_seconds()
    
    if time_diff_s == 0:
        return None
    
    # Velocidade em m/s, converter para km/h
    speed_ms = distance_m / time_diff_s
    speed_kmh = speed_ms * 3.6
    
    return speed_kmh


def calculate_heading(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calcula direção (bearing) entre dois pontos.
    
    Args:
        lat1, lon1: Coordenadas do ponto 1
        lat2, lon2: Coordenadas do ponto 2
    
    Returns:
        Direção em graus (0-360), onde 0=Norte, 90=Leste, 180=Sul, 270=Oeste
    
    Example:
        >>> heading = calculate_heading(-23.5505, -46.6333, -23.5489, -46.6388)
        >>> print(f"Direção: {heading:.1f}°")
    """
    # Converter para radianos
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    dlon_rad = math.radians(lon2 - lon1)
    
    # Calcular bearing
    x = math.sin(dlon_rad) * math.cos(lat2_rad)
    y = math.cos(lat1_rad) * math.sin(lat2_rad) - \
        math.sin(lat1_rad) * math.cos(lat2_rad) * math.cos(dlon_rad)
    
    bearing_rad = math.atan2(x, y)
    bearing_deg = math.degrees(bearing_rad)
    
    # Normalizar para 0-360
    bearing_deg = (bearing_deg + 360) % 360
    
    return bearing_deg


def is_coordinate_valid(lat: float, lon: float, strict: bool = False) -> bool:
    """
    Valida se coordenadas são válidas.
    
    Args:
        lat: Latitude
        lon: Longitude
        strict: Se True, valida também se está dentro de São Paulo
    
    Returns:
        True se válido
    
    Example:
        >>> is_coordinate_valid(-23.5505, -46.6333)
        True
        >>> is_coordinate_valid(100, 200)
        False
    """
    # Validação básica
    if not (-90 <= lat <= 90):
        return False
    if not (-180 <= lon <= 180):
        return False
    
    # Validação estrita (São Paulo)
    if strict:
        if not (-24.0 <= lat <= -23.0):
            return False
        if not (-47.0 <= lon <= -46.0):
            return False
    
    return True


def get_current_timestamp(timezone: str = 'America/Sao_Paulo') -> datetime:
    """
    Retorna timestamp atual com timezone.
    
    Args:
        timezone: Timezone (padrão: America/Sao_Paulo)
    
    Returns:
        Datetime com timezone
    
    Example:
        >>> now = get_current_timestamp()
        >>> print(now.strftime('%Y-%m-%d %H:%M:%S %Z'))
    """
    tz = pytz.timezone(timezone)
    return datetime.now(tz)


def parse_timestamp(timestamp_str: str, timezone: str = 'America/Sao_Paulo') -> datetime:
    """
    Faz parse de timestamp string.
    
    Args:
        timestamp_str: String no formato "YYYY-MM-DD HH:MM:SS"
        timezone: Timezone para localizar
    
    Returns:
        Datetime com timezone
    
    Example:
        >>> dt = parse_timestamp("2025-10-20 14:30:00")
    """
    # Parse sem timezone
    dt_naive = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
    
    # Adicionar timezone
    tz = pytz.timezone(timezone)
    dt_aware = tz.localize(dt_naive)
    
    return dt_aware


def get_partition_path(base_path: str, dt: datetime) -> str:
    """
    Gera caminho particionado por data/hora (Hive-style).
    
    Args:
        base_path: Caminho base
        dt: Datetime para particionar
    
    Returns:
        Caminho com partições
    
    Example:
        >>> path = get_partition_path("s3a://bronze/positions/", datetime(2025, 10, 20, 14, 30))
        >>> print(path)
        s3a://bronze/positions/year=2025/month=10/day=20/hour=14/
    """
    return (
        f"{base_path}"
        f"year={dt.year}/"
        f"month={dt.month:02d}/"
        f"day={dt.day:02d}/"
        f"hour={dt.hour:02d}/"
    )


def round_to_nearest_minute(dt: datetime, minutes: int = 15) -> datetime:
    """
    Arredonda datetime para o minuto mais próximo.
    
    Args:
        dt: Datetime para arredondar
        minutes: Minutos para arredondar (ex: 15 para quarters)
    
    Returns:
        Datetime arredondado
    
    Example:
        >>> dt = datetime(2025, 10, 20, 14, 37, 30)
        >>> rounded = round_to_nearest_minute(dt, 15)
        >>> print(rounded)  # 2025-10-20 14:30:00
    """
    # Calcular minutos desde meia-noite
    total_minutes = dt.hour * 60 + dt.minute
    
    # Arredondar
    rounded_minutes = (total_minutes // minutes) * minutes
    
    # Criar novo datetime
    return dt.replace(
        hour=rounded_minutes // 60,
        minute=rounded_minutes % 60,
        second=0,
        microsecond=0
    )


def format_size(size_bytes: int) -> str:
    """
    Formata tamanho em bytes para formato legível.
    
    Args:
        size_bytes: Tamanho em bytes
    
    Returns:
        String formatada (ex: "1.5 GB")
    
    Example:
        >>> print(format_size(1536000000))
        1.43 GB
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"


def format_duration(seconds: float) -> str:
    """
    Formata duração em segundos para formato legível.
    
    Args:
        seconds: Duração em segundos
    
    Returns:
        String formatada (ex: "1h 23m 45s")
    
    Example:
        >>> print(format_duration(5025))
        1h 23m 45s
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    
    minutes = int(seconds // 60)
    remaining_seconds = seconds % 60
    
    if minutes < 60:
        return f"{minutes}m {remaining_seconds:.1f}s"
    
    hours = minutes // 60
    remaining_minutes = minutes % 60
    
    return f"{hours}h {remaining_minutes}m {remaining_seconds:.1f}s"


def sanitize_string(s: str, max_length: int = 100) -> str:
    """
    Sanitiza string para uso seguro.
    
    Args:
        s: String para sanitizar
        max_length: Comprimento máximo
    
    Returns:
        String sanitizada
    
    Example:
        >>> sanitize_string("Hello\nWorld\t!")
        'Hello World !'
    """
    if not s:
        return ""
    
    # Remover caracteres de controle
    s = s.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    
    # Remover espaços múltiplos
    s = ' '.join(s.split())
    
    # Truncar
    if len(s) > max_length:
        s = s[:max_length] + "..."
    
    return s.strip()


def retry_with_backoff(
    func,
    max_retries: int = 3,
    initial_delay: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: Tuple = (Exception,)
):
    """
    Executa função com retry e exponential backoff.
    
    Args:
        func: Função para executar
        max_retries: Número máximo de tentativas
        initial_delay: Delay inicial em segundos
        backoff_factor: Fator de multiplicação do delay
        exceptions: Tupla de exceções para capturar
    
    Returns:
        Resultado da função
    
    Example:
        >>> result = retry_with_backoff(lambda: api_call(), max_retries=3)
    """
    import time
    
    delay = initial_delay
    last_exception = None
    
    for attempt in range(max_retries):
        try:
            return func()
        except exceptions as e:
            last_exception = e
            if attempt < max_retries - 1:
                time.sleep(delay)
                delay *= backoff_factor
            else:
                raise last_exception


if __name__ == '__main__':
    # Testes
    print("=== Testes de Utilidades ===\n")
    
    # Distância
    dist = haversine_distance(-23.5505, -46.6333, -23.5489, -46.6388)
    print(f"Distância Haversine: {dist:.2f} metros")
    
    # Velocidade
    t1 = datetime(2025, 10, 20, 14, 0, 0)
    t2 = datetime(2025, 10, 20, 14, 5, 0)
    speed = calculate_speed(-23.5505, -46.6333, -23.5489, -46.6388, t1, t2)
    print(f"Velocidade: {speed:.2f} km/h")
    
    # Direção
    heading = calculate_heading(-23.5505, -46.6333, -23.5489, -46.6388)
    print(f"Direção: {heading:.1f}°")
    
    # Validação
    print(f"Coordenada válida: {is_coordinate_valid(-23.5505, -46.6333)}")
    print(f"Coordenada inválida: {is_coordinate_valid(100, 200)}")
    
    # Formatação
    print(f"Tamanho: {format_size(1536000000)}")
    print(f"Duração: {format_duration(5025)}")
    
    # Partição
    now = datetime(2025, 10, 20, 14, 30)
    path = get_partition_path("s3a://bronze/positions/", now)
    print(f"Path particionado: {path}")
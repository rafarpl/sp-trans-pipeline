"""
Schema Definitions Module
=========================
Definições de schemas Spark para dados GTFS e API SPTrans.

Garante consistência de tipos de dados e facilita validação.
"""

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType, DoubleType,
    BooleanType, TimestampType, DateType
)

from src.common.logging_config import get_logger

logger = get_logger(__name__)


class GTFSSchemas:
    """Schemas para arquivos GTFS."""
    
    def __init__(self):
        """Inicializa schemas GTFS."""
        logger.info("Inicializando schemas GTFS")
        self._init_schemas()
    
    def _init_schemas(self):
        """Define todos os schemas GTFS."""
        
        # agency.txt
        self.agency_schema = StructType([
            StructField("agency_id", StringType(), True),
            StructField("agency_name", StringType(), True),
            StructField("agency_url", StringType(), True),
            StructField("agency_timezone", StringType(), True),
            StructField("agency_lang", StringType(), True),
            StructField("agency_phone", StringType(), True),
        ])
        
        # routes.txt
        self.routes_schema = StructType([
            StructField("route_id", StringType(), True),
            StructField("agency_id", StringType(), True),
            StructField("route_short_name", StringType(), True),
            StructField("route_long_name", StringType(), True),
            StructField("route_desc", StringType(), True),
            StructField("route_type", IntegerType(), True),
            StructField("route_url", StringType(), True),
            StructField("route_color", StringType(), True),
            StructField("route_text_color", StringType(), True),
        ])
        
        # trips.txt
        self.trips_schema = StructType([
            StructField("route_id", StringType(), True),
            StructField("service_id", StringType(), True),
            StructField("trip_id", StringType(), True),
            StructField("trip_headsign", StringType(), True),
            StructField("trip_short_name", StringType(), True),
            StructField("direction_id", IntegerType(), True),
            StructField("block_id", StringType(), True),
            StructField("shape_id", StringType(), True),
            StructField("wheelchair_accessible", IntegerType(), True),
            StructField("bikes_allowed", IntegerType(), True),
        ])
        
        # stops.txt
        self.stops_schema = StructType([
            StructField("stop_id", StringType(), True),
            StructField("stop_code", StringType(), True),
            StructField("stop_name", StringType(), True),
            StructField("stop_desc", StringType(), True),
            StructField("stop_lat", DoubleType(), True),
            StructField("stop_lon", DoubleType(), True),
            StructField("zone_id", StringType(), True),
            StructField("stop_url", StringType(), True),
            StructField("location_type", IntegerType(), True),
            StructField("parent_station", StringType(), True),
            StructField("stop_timezone", StringType(), True),
            StructField("wheelchair_boarding", IntegerType(), True),
        ])
        
        # stop_times.txt
        self.stop_times_schema = StructType([
            StructField("trip_id", StringType(), True),
            StructField("arrival_time", StringType(), True),
            StructField("departure_time", StringType(), True),
            StructField("stop_id", StringType(), True),
            StructField("stop_sequence", IntegerType(), True),
            StructField("stop_headsign", StringType(), True),
            StructField("pickup_type", IntegerType(), True),
            StructField("drop_off_type", IntegerType(), True),
            StructField("shape_dist_traveled", DoubleType(), True),
            StructField("timepoint", IntegerType(), True),
        ])
        
        # shapes.txt
        self.shapes_schema = StructType([
            StructField("shape_id", StringType(), True),
            StructField("shape_pt_lat", DoubleType(), True),
            StructField("shape_pt_lon", DoubleType(), True),
            StructField("shape_pt_sequence", IntegerType(), True),
            StructField("shape_dist_traveled", DoubleType(), True),
        ])
        
        # calendar.txt
        self.calendar_schema = StructType([
            StructField("service_id", StringType(), True),
            StructField("monday", IntegerType(), True),
            StructField("tuesday", IntegerType(), True),
            StructField("wednesday", IntegerType(), True),
            StructField("thursday", IntegerType(), True),
            StructField("friday", IntegerType(), True),
            StructField("saturday", IntegerType(), True),
            StructField("sunday", IntegerType(), True),
            StructField("start_date", StringType(), True),
            StructField("end_date", StringType(), True),
        ])
        
        # calendar_dates.txt (opcional)
        self.calendar_dates_schema = StructType([
            StructField("service_id", StringType(), True),
            StructField("date", StringType(), True),
            StructField("exception_type", IntegerType(), True),
        ])
        
        # fare_attributes.txt (opcional)
        self.fare_attributes_schema = StructType([
            StructField("fare_id", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("currency_type", StringType(), True),
            StructField("payment_method", IntegerType(), True),
            StructField("transfers", IntegerType(), True),
            StructField("transfer_duration", IntegerType(), True),
        ])
        
        # fare_rules.txt (opcional)
        self.fare_rules_schema = StructType([
            StructField("fare_id", StringType(), True),
            StructField("route_id", StringType(), True),
            StructField("origin_id", StringType(), True),
            StructField("destination_id", StringType(), True),
            StructField("contains_id", StringType(), True),
        ])
        
        logger.info("Schemas GTFS inicializados")
    
    def get_schema(self, table_name: str) -> StructType:
        """
        Retorna schema para uma tabela específica.
        
        Args:
            table_name: Nome da tabela (ex: 'routes', 'stops')
        
        Returns:
            Schema Spark
        
        Raises:
            ValueError: Se tabela não existir
        """
        schema_attr = f"{table_name}_schema"
        
        if hasattr(self, schema_attr):
            return getattr(self, schema_attr)
        else:
            raise ValueError(f"Schema não encontrado para tabela: {table_name}")


class SPTransAPISchemas:
    """Schemas para dados da API SPTrans."""
    
    def __init__(self):
        """Inicializa schemas da API."""
        logger.info("Inicializando schemas API SPTrans")
        self._init_schemas()
    
    def _init_schemas(self):
        """Define schemas da API."""
        
        # Schema para posições de veículos
        self.positions_schema = StructType([
            StructField("vehicle_id", StringType(), False),  # Prefixo do veículo
            StructField("route_code", StringType(), True),   # Código da linha
            StructField("route_name", StringType(), True),   # Nome da linha
            StructField("direction", IntegerType(), True),   # Sentido (1 ou 2)
            StructField("latitude", DoubleType(), False),    # Latitude
            StructField("longitude", DoubleType(), False),   # Longitude
            StructField("timestamp", TimestampType(), False), # Timestamp da posição
            StructField("has_accessibility", BooleanType(), True), # Acessibilidade
            StructField("timestamp_capture", TimestampType(), True), # Timestamp da captura
        ])
        
        # Schema para linhas (routes)
        self.lines_schema = StructType([
            StructField("line_code", StringType(), False),
            StructField("is_circular", BooleanType(), True),
            StructField("sign", StringType(), True),
            StructField("direction", IntegerType(), True),
            StructField("line_type", IntegerType(), True),
            StructField("main_terminal", StringType(), True),
            StructField("secondary_terminal", StringType(), True),
        ])
        
        # Schema para paradas (stops)
        self.stops_schema = StructType([
            StructField("stop_code", StringType(), False),
            StructField("stop_name", StringType(), True),
            StructField("address", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
        ])
        
        # Schema para previsões
        self.forecast_schema = StructType([
            StructField("stop_code", StringType(), False),
            StructField("line_code", StringType(), False),
            StructField("vehicle_id", StringType(), True),
            StructField("arrival_time", StringType(), True),
            StructField("has_accessibility", BooleanType(), True),
        ])
        
        logger.info("Schemas API SPTrans inicializados")
    
    def get_schema(self, schema_name: str) -> StructType:
        """
        Retorna schema específico.
        
        Args:
            schema_name: Nome do schema (ex: 'positions', 'lines')
        
        Returns:
            Schema Spark
        """
        schema_attr = f"{schema_name}_schema"
        
        if hasattr(self, schema_attr):
            return getattr(self, schema_attr)
        else:
            raise ValueError(f"Schema não encontrado: {schema_name}")


class SilverSchemas:
    """Schemas para camada Silver (dados processados)."""
    
    def __init__(self):
        """Inicializa schemas Silver."""
        logger.info("Inicializando schemas Silver")
        self._init_schemas()
    
    def _init_schemas(self):
        """Define schemas Silver."""
        
        # Posições enriquecidas
        self.positions_enriched_schema = StructType([
            # Dados originais
            StructField("vehicle_id", StringType(), False),
            StructField("route_code", StringType(), True),
            StructField("latitude", DoubleType(), False),
            StructField("longitude", DoubleType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("has_accessibility", BooleanType(), True),
            
            # Validações
            StructField("is_valid_coordinate", BooleanType(), True),
            StructField("is_valid_timestamp", BooleanType(), True),
            StructField("is_valid_vehicle_id", BooleanType(), True),
            StructField("is_valid", BooleanType(), True),
            
            # Campos derivados
            StructField("date", DateType(), True),
            StructField("hour", IntegerType(), True),
            StructField("day_of_week", IntegerType(), True),
            StructField("is_weekend", BooleanType(), True),
            StructField("speed_kmh", DoubleType(), True),
            StructField("is_moving", BooleanType(), True),
            
            # Enriquecimento GTFS
            StructField("route_short_name", StringType(), True),
            StructField("route_long_name", StringType(), True),
            StructField("route_type", IntegerType(), True),
            StructField("has_route_info", BooleanType(), True),
            
            # Geocoding (se aplicado)
            StructField("address_street", StringType(), True),
            StructField("address_neighborhood", StringType(), True),
            StructField("address_city", StringType(), True),
            
            # Metadados
            StructField("ingestion_timestamp", TimestampType(), True),
            StructField("silver_processing_timestamp", TimestampType(), True),
            StructField("silver_processing_date", DateType(), True),
            StructField("data_quality_score", DoubleType(), True),
            StructField("layer", StringType(), True),
        ])
        
        logger.info("Schemas Silver inicializados")


class GoldSchemas:
    """Schemas para camada Gold (dados agregados)."""
    
    def __init__(self):
        """Inicializa schemas Gold."""
        logger.info("Inicializando schemas Gold")
        self._init_schemas()
    
    def _init_schemas(self):
        """Define schemas Gold."""
        
        # KPIs por hora
        self.kpis_hourly_schema = StructType([
            StructField("date", DateType(), False),
            StructField("hour", IntegerType(), False),
            StructField("route_code", StringType(), True),
            StructField("total_vehicles", IntegerType(), True),
            StructField("vehicles_moving", IntegerType(), True),
            StructField("vehicles_stopped", IntegerType(), True),
            StructField("avg_speed_kmh", DoubleType(), True),
            StructField("max_speed_kmh", DoubleType(), True),
            StructField("total_records", IntegerType(), True),
            StructField("data_quality_score", DoubleType(), True),
        ])
        
        # Métricas por rota
        self.metrics_by_route_schema = StructType([
            StructField("date", DateType(), False),
            StructField("route_code", StringType(), False),
            StructField("route_name", StringType(), True),
            StructField("total_trips", IntegerType(), True),
            StructField("total_vehicles", IntegerType(), True),
            StructField("avg_speed", DoubleType(), True),
            StructField("distance_traveled_km", DoubleType(), True),
            StructField("service_hours", DoubleType(), True),
        ])
        
        # Análise de headway (intervalo entre ônibus)
        self.headway_analysis_schema = StructType([
            StructField("date", DateType(), False),
            StructField("hour", IntegerType(), False),
            StructField("route_code", StringType(), False),
            StructField("stop_id", StringType(), True),
            StructField("avg_headway_minutes", DoubleType(), True),
            StructField("min_headway_minutes", DoubleType(), True),
            StructField("max_headway_minutes", DoubleType(), True),
            StructField("headway_variance", DoubleType(), True),
        ])
        
        logger.info("Schemas Gold inicializados")


# Instâncias globais para uso fácil
gtfs_schemas = GTFSSchemas()
api_schemas = SPTransAPISchemas()
silver_schemas = SilverSchemas()
gold_schemas = GoldSchemas()


if __name__ == "__main__":
    # Teste dos schemas
    print("=== Testando Schemas ===\n")
    
    print("1. GTFS Schemas:")
    print(f"   - Routes: {len(gtfs_schemas.routes_schema.fields)} campos")
    print(f"   - Stops: {len(gtfs_schemas.stops_schema.fields)} campos")
    print(f"   - Trips: {len(gtfs_schemas.trips_schema.fields)} campos")
    
    print("\n2. API Schemas:")
    print(f"   - Positions: {len(api_schemas.positions_schema.fields)} campos")
    print(f"   - Lines: {len(api_schemas.lines_schema.fields)} campos")
    
    print("\n3. Silver Schemas:")
    print(f"   - Positions Enriched: {len(silver_schemas.positions_enriched_schema.fields)} campos")
    
    print("\n4. Gold Schemas:")
    print(f"   - KPIs Hourly: {len(gold_schemas.kpis_hourly_schema.fields)} campos")
    print(f"   - Metrics by Route: {len(gold_schemas.metrics_by_route_schema.fields)} campos")
    
    print("\n✅ Todos os schemas carregados com sucesso!")

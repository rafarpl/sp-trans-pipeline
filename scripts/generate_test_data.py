#!/usr/bin/env python3
"""
Script: generate_test_data.py
Descrição: Gera dados sintéticos para testes do pipeline SPTrans
Projeto: SPTrans Real-Time Data Pipeline
Autor: Equipe LABDATA/FIA
"""

import json
import random
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any
import csv

# ============================================================================
# CONFIGURAÇÕES
# ============================================================================

PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "samples"
FIXTURES_DIR = PROJECT_ROOT / "tests" / "fixtures"

# Coordenadas aproximadas de São Paulo (centro)
SP_CENTER_LAT = -23.550520
SP_CENTER_LON = -46.633308
SP_RADIUS_KM = 30  # Raio de 30km do centro

# ============================================================================
# GERADORES DE DADOS
# ============================================================================

class TestDataGenerator:
    """Gerador de dados de teste para o pipeline SPTrans"""
    
    def __init__(self, seed: int = 42):
        """Inicializa o gerador com uma seed para reprodutibilidade"""
        random.seed(seed)
        self.route_codes = self._generate_route_codes()
        self.stop_ids = list(range(1000, 2000, 100))
    
    @staticmethod
    def _generate_route_codes() -> List[str]:
        """Gera códigos de linhas realistas"""
        prefixes = ['8000', '8100', '8200', '8300', '8400', '8500']
        return [f"{prefix}-{suffix}" for prefix in prefixes 
                for suffix in range(10, 20)]
    
    @staticmethod
    def _random_coordinate(center: float, radius_km: float) -> float:
        """Gera coordenada aleatória próxima ao centro"""
        # 1 grau ≈ 111km
        offset = (random.random() - 0.5) * 2 * (radius_km / 111)
        return center + offset
    
    def generate_api_positions_response(self, num_vehicles: int = 100) -> Dict[str, Any]:
        """
        Gera resposta da API de posições dos ônibus
        
        Args:
            num_vehicles: Número de veículos a gerar
            
        Returns:
            Dicionário simulando resposta da API SPTrans Olho Vivo
        """
        timestamp = datetime.now()
        
        vehicles = []
        for i in range(num_vehicles):
            vehicle = {
                "p": random.choice(self.route_codes),  # Linha (prefixo)
                "a": random.choice([True, False]),      # Acessibilidade
                "ta": timestamp.strftime("%Y-%m-%d %H:%M:%S"),  # Timestamp
                "py": self._random_coordinate(SP_CENTER_LAT, SP_RADIUS_KM),  # Latitude
                "px": self._random_coordinate(SP_CENTER_LON, SP_RADIUS_KM),  # Longitude
            }
            vehicles.append(vehicle)
        
        response = {
            "hr": timestamp.strftime("%H:%M"),
            "l": vehicles
        }
        
        return response
    
    def generate_gtfs_routes(self, num_routes: int = 50) -> List[Dict[str, str]]:
        """
        Gera dados GTFS de rotas
        
        Args:
            num_routes: Número de rotas a gerar
            
        Returns:
            Lista de dicionários representando rotas
        """
        route_types = {
            3: "Ônibus",
            1: "Metrô",
            2: "Trem"
        }
        
        routes = []
        for i in range(num_routes):
            route_code = self.route_codes[i % len(self.route_codes)]
            route = {
                "route_id": f"R{i+1:04d}",
                "agency_id": "SPTrans",
                "route_short_name": route_code,
                "route_long_name": f"Terminal A - Terminal B via {route_code}",
                "route_type": "3",
                "route_color": f"{random.randint(0, 255):02X}"
                              f"{random.randint(0, 255):02X}"
                              f"{random.randint(0, 255):02X}",
                "route_text_color": "FFFFFF"
            }
            routes.append(route)
        
        return routes
    
    def generate_gtfs_stops(self, num_stops: int = 200) -> List[Dict[str, str]]:
        """
        Gera dados GTFS de paradas
        
        Args:
            num_stops: Número de paradas a gerar
            
        Returns:
            Lista de dicionários representando paradas
        """
        stop_types = ["Ponto", "Terminal", "Estação"]
        
        stops = []
        for i in range(num_stops):
            stop = {
                "stop_id": f"S{i+1:05d}",
                "stop_code": f"{10000 + i}",
                "stop_name": f"Parada {stop_types[i % len(stop_types)]} {i+1}",
                "stop_desc": f"Descrição da parada {i+1}",
                "stop_lat": str(self._random_coordinate(SP_CENTER_LAT, SP_RADIUS_KM)),
                "stop_lon": str(self._random_coordinate(SP_CENTER_LON, SP_RADIUS_KM)),
                "zone_id": f"Z{(i % 10) + 1}",
                "stop_url": "",
                "location_type": "0",
                "parent_station": ""
            }
            stops.append(stop)
        
        return stops
    
    def generate_gtfs_trips(self, routes: List[Dict], num_trips_per_route: int = 5) -> List[Dict[str, str]]:
        """
        Gera dados GTFS de viagens
        
        Args:
            routes: Lista de rotas
            num_trips_per_route: Número de viagens por rota
            
        Returns:
            Lista de dicionários representando viagens
        """
        trips = []
        trip_counter = 1
        
        for route in routes:
            for trip_num in range(num_trips_per_route):
                trip = {
                    "route_id": route["route_id"],
                    "service_id": f"S{random.randint(1, 3)}",
                    "trip_id": f"T{trip_counter:06d}",
                    "trip_headsign": route["route_long_name"].split(" - ")[1],
                    "trip_short_name": f"Trip {trip_num + 1}",
                    "direction_id": str(random.randint(0, 1)),
                    "block_id": f"B{random.randint(1, 100):03d}",
                    "shape_id": f"SH{trip_counter:05d}",
                    "wheelchair_accessible": str(random.randint(0, 2)),
                    "bikes_allowed": str(random.randint(0, 2))
                }
                trips.append(trip)
                trip_counter += 1
        
        return trips
    
    def generate_gtfs_stop_times(self, trips: List[Dict], stops: List[Dict]) -> List[Dict[str, str]]:
        """
        Gera dados GTFS de horários de parada
        
        Args:
            trips: Lista de viagens
            stops: Lista de paradas
            
        Returns:
            Lista de dicionários representando horários de parada
        """
        stop_times = []
        
        for trip in trips:
            num_stops_in_trip = random.randint(10, 30)
            selected_stops = random.sample(stops, min(num_stops_in_trip, len(stops)))
            
            # Horário inicial da viagem
            base_time = datetime.combine(datetime.today(), 
                                        datetime.min.time()) + timedelta(hours=random.randint(5, 22))
            
            for seq, stop in enumerate(selected_stops, start=1):
                # Incrementar tempo entre paradas (2-5 minutos)
                arrival_time = base_time + timedelta(minutes=random.randint(2, 5) * seq)
                departure_time = arrival_time + timedelta(seconds=random.randint(30, 120))
                
                stop_time = {
                    "trip_id": trip["trip_id"],
                    "arrival_time": arrival_time.strftime("%H:%M:%S"),
                    "departure_time": departure_time.strftime("%H:%M:%S"),
                    "stop_id": stop["stop_id"],
                    "stop_sequence": str(seq),
                    "stop_headsign": "",
                    "pickup_type": "0",
                    "drop_off_type": "0",
                    "shape_dist_traveled": f"{seq * random.uniform(0.5, 2.0):.2f}"
                }
                stop_times.append(stop_time)
        
        return stop_times
    
    def generate_spark_test_data(self) -> Dict[str, List[Dict]]:
        """
        Gera conjunto completo de dados para testes do Spark
        
        Returns:
            Dicionário com todos os datasets de teste
        """
        routes = self.generate_gtfs_routes(20)
        stops = self.generate_gtfs_stops(50)
        trips = self.generate_gtfs_trips(routes, 3)
        stop_times = self.generate_gtfs_stop_times(trips, stops)
        
        return {
            "routes": routes,
            "stops": stops,
            "trips": trips,
            "stop_times": stop_times,
            "api_positions": [self.generate_api_positions_response(50) for _ in range(5)]
        }

# ============================================================================
# FUNÇÕES DE SALVAMENTO
# ============================================================================

def save_json(data: Any, filepath: Path):
    """Salva dados em formato JSON"""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"✓ Arquivo salvo: {filepath}")

def save_csv(data: List[Dict], filepath: Path):
    """Salva dados em formato CSV"""
    if not data:
        print(f"⚠ Nenhum dado para salvar em {filepath}")
        return
    
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    
    print(f"✓ Arquivo salvo: {filepath}")

# ============================================================================
# FUNÇÃO PRINCIPAL
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Gera dados de teste para o pipeline SPTrans"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(DATA_DIR),
        help="Diretório de saída para os dados gerados"
    )
    parser.add_argument(
        "--fixtures",
        action="store_true",
        help="Gera dados para fixtures de testes"
    )
    parser.add_argument(
        "--num-vehicles",
        type=int,
        default=100,
        help="Número de veículos a gerar"
    )
    parser.add_argument(
        "--num-routes",
        type=int,
        default=50,
        help="Número de rotas a gerar"
    )
    parser.add_argument(
        "--num-stops",
        type=int,
        default=200,
        help="Número de paradas a gerar"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Seed para reprodutibilidade"
    )
    
    args = parser.parse_args()
    
    print("\n" + "="*70)
    print("  SPTrans Data Pipeline - Gerador de Dados de Teste")
    print("="*70 + "\n")
    
    # Inicializar gerador
    generator = TestDataGenerator(seed=args.seed)
    
    # Definir diretório de saída
    output_dir = Path(args.output_dir)
    if args.fixtures:
        output_dir = FIXTURES_DIR
    
    print(f"Diretório de saída: {output_dir}\n")
    
    # Gerar dados da API
    print("📍 Gerando dados da API (posições de ônibus)...")
    api_response = generator.generate_api_positions_response(args.num_vehicles)
    save_json(api_response, output_dir / "sample_api_response.json")
    
    # Gerar dados GTFS
    print("\n🚌 Gerando dados GTFS...")
    
    print("  → Rotas...")
    routes = generator.generate_gtfs_routes(args.num_routes)
    save_csv(routes, output_dir / "sample_gtfs_routes.txt")
    
    print("  → Paradas...")
    stops = generator.generate_gtfs_stops(args.num_stops)
    save_csv(stops, output_dir / "sample_gtfs_stops.txt")
    
    print("  → Viagens...")
    trips = generator.generate_gtfs_trips(routes, num_trips_per_route=3)
    save_csv(trips, output_dir / "sample_gtfs_trips.txt")
    
    print("  → Horários de parada...")
    stop_times = generator.generate_gtfs_stop_times(trips, stops)
    save_csv(stop_times, output_dir / "sample_gtfs_stop_times.txt")
    
    # Gerar dados para testes Spark
    if args.fixtures:
        print("\n⚡ Gerando dados para testes Spark...")
        spark_data = generator.generate_spark_test_data()
        save_json(spark_data, output_dir / "mock_spark_data.json")
    
    # Resumo
    print("\n" + "="*70)
    print("✅ GERAÇÃO CONCLUÍDA COM SUCESSO!")
    print("="*70)
    print(f"\nEstatísticas:")
    print(f"  • Veículos gerados: {args.num_vehicles}")
    print(f"  • Rotas geradas: {len(routes)}")
    print(f"  • Paradas geradas: {len(stops)}")
    print(f"  • Viagens geradas: {len(trips)}")
    print(f"  • Horários de parada: {len(stop_times)}")
    print(f"\nArquivos salvos em: {output_dir}")
    print("\n" + "="*70 + "\n")

if __name__ == "__main__":
    main()

import sys
import os

# Ajustar path para Windows
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.ingestion.sptrans_api_client import SPTransAPIClient

print("🧪 Testando API SPTrans...")
client = SPTransAPIClient()

if client.authenticate():
    print("✅ Autenticação OK!")
    positions = client.get_vehicle_positions()
    print(f"✅ {len(positions)} veículos recebidos!")
    if positions:
        print(f"📊 Exemplo: {positions[0]}")
else:
    print("❌ Falha na autenticação")
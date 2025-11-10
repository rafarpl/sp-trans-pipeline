"""
SPTrans API Client
Cliente simplificado e funcional para a API Olho Vivo da SPTrans
"""

import sys
import os

# Adicionar o diretório raiz ao path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import logging
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional

from src.common.config import get_config
from src.common.exceptions import (
    APIException,
    APIAuthenticationException,
    APIRateLimitException
)

logger = logging.getLogger(__name__)


class SPTransAPIClient:
    """
    Cliente para API SPTrans Olho Vivo
    
    Documentação: https://www.sptrans.com.br/desenvolvedores/
    """
    
    def __init__(self):
        """Inicializa o cliente da API"""
        self.logger = logging.getLogger(__name__)
        self._authenticated = False
        self.auth_expires_at = None
        
        # Carregar configuração
        self.config = get_config()
        self.api_token = self.config.SPTRANS_API_TOKEN
        self.base_url = self.config.SPTRANS_API_BASE_URL
        
        # Configurar sessão HTTP
        self.session = requests.Session()
        self.timeout = 30
        
        self.logger.info("SPTransAPIClient initialized")
    
    def _is_authenticated(self) -> bool:
        """Verifica se ainda está autenticado"""
        if not self._authenticated:
            return False
        
        if self.auth_expires_at and datetime.now() >= self.auth_expires_at:
            self._authenticated = False
            return False
        
        return True
    
    def authenticate(self, force: bool = False) -> bool:
        """
        Autentica na API SPTrans
        
        Args:
            force: Forçar nova autenticação
            
        Returns:
            True se autenticado com sucesso
        """
        # Verificar se já está autenticado
        if not force and self._is_authenticated():
            self.logger.debug("Already authenticated")
            return True
        
        try:
            self.logger.info("Authenticating with SPTrans API")
            
            url = f"{self.base_url}/Login/Autenticar?token={self.api_token}"
            headers = {"Content-Length": "0"}
            
            response = self.session.post(url, headers=headers, timeout=self.timeout)
            
            if response.status_code == 200:
                result = response.text.strip().lower()
                
                if result == "true":
                    self._authenticated = True
                    self.auth_expires_at = datetime.now() + timedelta(hours=2)
                    self.logger.info("✅ Authentication successful")
                    return True
                else:
                    self.logger.error(f"Authentication returned: {result}")
                    return False
            else:
                self.logger.error(f"Authentication failed: HTTP {response.status_code}")
                return False
                
        except requests.exceptions.Timeout:
            self.logger.error("Authentication timeout")
            return False
        except requests.exceptions.ConnectionError:
            self.logger.error("Connection error during authentication")
            return False
        except Exception as e:
            self.logger.error(f"Authentication error: {e}")
            return False
    
    def get_vehicle_positions(self) -> List[Dict]:
        """
        Busca posições de todos os veículos
        
        Returns:
            Lista de dicionários com dados dos veículos
        """
        if not self._is_authenticated():
            if not self.authenticate():
                self.logger.error("Cannot get positions: not authenticated")
                return []
        
        try:
            url = f"{self.base_url}/Posicao"
            response = self.session.get(url, timeout=self.timeout)
            
            if response.status_code == 200:
                data = response.json()
                
                # Processar resposta da API
                vehicles = []
                
                if isinstance(data, dict) and 'l' in data:
                    # Formato: {"hr": "...", "l": [...]}
                    for line in data.get('l', []):
                        line_id = str(line.get('c', ''))  # Código da linha
                        
                        for vehicle in line.get('vs', []):
                            vehicles.append({
                                'vehicle_id': str(vehicle.get('p', '')),  # Prefixo
                                'line_id': line_id,
                                'latitude': float(vehicle.get('py', 0)),
                                'longitude': float(vehicle.get('px', 0)),
                                'timestamp': datetime.now(),
                                'speed': 0.0,  # API não retorna velocidade
                                'route_id': line_id
                            })
                
                self.logger.info(f"Retrieved {len(vehicles)} vehicle positions")
                return vehicles
            
            else:
                self.logger.error(f"Failed to get positions: HTTP {response.status_code}")
                return []
                
        except requests.exceptions.Timeout:
            self.logger.error("Timeout getting vehicle positions")
            return []
        except requests.exceptions.ConnectionError:
            self.logger.error("Connection error getting positions")
            return []
        except Exception as e:
            self.logger.error(f"Error getting positions: {e}")
            return []
    
    def get_lines(self) -> List[Dict]:
        """
        Busca todas as linhas
        
        Returns:
            Lista de linhas
        """
        if not self._is_authenticated():
            if not self.authenticate():
                return []
        
        try:
            url = f"{self.base_url}/Linha/Buscar?termosBusca="
            response = self.session.get(url, timeout=self.timeout)
            
            if response.status_code == 200:
                lines = response.json()
                self.logger.info(f"Retrieved {len(lines)} lines")
                return lines
            else:
                self.logger.error(f"Failed to get lines: HTTP {response.status_code}")
                return []
                
        except Exception as e:
            self.logger.error(f"Error getting lines: {e}")
            return []
    
    def get_line_details(self, line_id: int) -> Optional[Dict]:
        """
        Busca detalhes de uma linha específica
        
        Args:
            line_id: ID da linha
            
        Returns:
            Detalhes da linha ou None
        """
        if not self._is_authenticated():
            if not self.authenticate():
                return None
        
        try:
            url = f"{self.base_url}/Linha/Carregar?codigoLinha={line_id}"
            response = self.session.get(url, timeout=self.timeout)
            
            if response.status_code == 200:
                return response.json()
            else:
                return None
                
        except Exception as e:
            self.logger.error(f"Error getting line details: {e}")
            return None


# Função helper para uso rápido
def get_current_positions() -> List[Dict]:
    """
    Helper function para buscar posições atuais rapidamente
    
    Returns:
        Lista de posições de veículos
    """
    client = SPTransAPIClient()
    
    if client.authenticate():
        return client.get_vehicle_positions()
    
    return []


if __name__ == "__main__":
    """Teste rápido do cliente"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("="*60)
    print("🧪 TESTE DO CLIENTE SPTRANS")
    print("="*60)
    
    client = SPTransAPIClient()
    
    print("\n🔐 Autenticando...")
    if client.authenticate():
        print("✅ Autenticação OK!")
        
        print("\n📍 Buscando posições...")
        positions = client.get_vehicle_positions()
        
        if positions:
            print(f"✅ {len(positions)} veículos encontrados!")
            print(f"\n📊 Exemplo do primeiro veículo:")
            v = positions[0]
            for key, value in v.items():
                print(f"   {key}: {value}")
        else:
            print("⚠️  Nenhum veículo retornado")
    else:
        print("❌ Falha na autenticação")
    
    print("="*60)

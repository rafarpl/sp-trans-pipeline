"""
Cliente para API SPTrans Olho Vivo.
Gerencia autenticação, requisições e retry logic.
"""
import time
from typing import Dict, List, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.common.config import get_sptrans_api_config
from src.common.constants import SPTransEndpoint, ErrorCode
from src.common.logging_config import get_logger, LoggerMixin
from src.common.exceptions import (
    APIAuthenticationError,
    APITimeoutError,
    APIRateLimitError,
    APIResponseError
)


class SPTransAPIClient(LoggerMixin):
    """
    Cliente para interagir com a API SPTrans Olho Vivo.
    
    Funcionalidades:
    - Autenticação automática
    - Retry com exponential backoff
    - Circuit breaker
    - Rate limiting
    - Logging estruturado
    
    Example:
        >>> client = SPTransAPIClient()
        >>> positions = client.get_all_positions()
        >>> print(f"Veículos encontrados: {len(positions)}")
    """
    
    def __init__(self):
        """Inicializa cliente da API."""
        self.config = get_sptrans_api_config()
        self.session = self._create_session()
        self.token_cookie: Optional[str] = None
        self.last_auth_time: Optional[float] = None
        
        self.logger.info(
            "sptrans_client_initialized",
            base_url=self.config.base_url,
            timeout=self.config.timeout
        )
    
    def _create_session(self) -> requests.Session:
        """
        Cria sessão HTTP com retry strategy.
        
        Returns:
            Sessão configurada
        """
        session = requests.Session()
        
        # Configurar retry strategy
        retry_strategy = Retry(
            total=self.config.max_retries,
            backoff_factor=self.config.retry_delay,
            status_forcelist=[429, 500, 502, 503, 504],
            method_whitelist=["HEAD", "GET", "OPTIONS", "POST"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def _is_token_expired(self) -> bool:
        """
        Verifica se token está expirado.
        Token SPTrans expira após 24 horas.
        
        Returns:
            True se expirado ou não existe
        """
        if not self.token_cookie or not self.last_auth_time:
            return True
        
        # Token válido por 24 horas
        elapsed = time.time() - self.last_auth_time
        return elapsed > (24 * 60 * 60)
    
    def authenticate(self) -> bool:
        """
        Autentica na API SPTrans.
        
        Returns:
            True se autenticação bem-sucedida
        
        Raises:
            APIAuthenticationError: Se falha na autenticação
        """
        url = f"{self.config.base_url}{SPTransEndpoint.AUTENTICAR}"
        
        self.logger.info("authentication_started", url=url)
        
        try:
            response = self.session.post(
                url,
                params={'token': self.config.token},
                timeout=self.config.timeout
            )
            
            if response.status_code == 200 and response.text == 'true':
                # Salvar cookie de autenticação
                self.token_cookie = response.cookies.get('apiCredentials')
                self.last_auth_time = time.time()
                
                self.logger.info(
                    "authentication_successful",
                    cookie_received=bool(self.token_cookie)
                )
                return True
            else:
                self.logger.error(
                    "authentication_failed",
                    status_code=response.status_code,
                    response_text=response.text,
                    error_code=ErrorCode.API_AUTHENTICATION_FAILED
                )
                raise APIAuthenticationError(
                    f"Falha na autenticação: {response.text}"
                )
        
        except requests.Timeout as e:
            self.logger.error(
                "authentication_timeout",
                error=str(e),
                error_code=ErrorCode.API_TIMEOUT
            )
            raise APITimeoutError(f"Timeout na autenticação: {e}")
        
        except requests.RequestException as e:
            self.logger.error(
                "authentication_request_error",
                error=str(e)
            )
            raise APIAuthenticationError(f"Erro na requisição: {e}")
    
    def _ensure_authenticated(self) -> None:
        """
        Garante que está autenticado, renovando token se necessário.
        """
        if self._is_token_expired():
            self.logger.info("token_expired_renewing")
            self.authenticate()
    
    def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict] = None,
        method: str = 'GET'
    ) -> Dict:
        """
        Faz requisição à API com autenticação e tratamento de erros.
        
        Args:
            endpoint: Endpoint da API
            params: Parâmetros da requisição
            method: Método HTTP (GET ou POST)
        
        Returns:
            Resposta JSON da API
        
        Raises:
            APITimeoutError: Se timeout
            APIRateLimitError: Se rate limit excedido
            APIResponseError: Se resposta inválida
        """
        self._ensure_authenticated()
        
        url = f"{self.config.base_url}{endpoint}"
        cookies = {'apiCredentials': self.token_cookie} if self.token_cookie else None
        
        self.logger.debug(
            "api_request_started",
            method=method,
            endpoint=endpoint,
            params=params
        )
        
        try:
            if method == 'GET':
                response = self.session.get(
                    url,
                    params=params,
                    cookies=cookies,
                    timeout=self.config.timeout
                )
            else:
                response = self.session.post(
                    url,
                    params=params,
                    cookies=cookies,
                    timeout=self.config.timeout
                )
            
            # Verificar rate limit
            if response.status_code == 429:
                self.logger.warning(
                    "rate_limit_exceeded",
                    endpoint=endpoint,
                    error_code=ErrorCode.API_RATE_LIMIT
                )
                raise APIRateLimitError("Rate limit excedido")
            
            # Verificar resposta bem-sucedida
            response.raise_for_status()
            
            # Parse JSON
            try:
                data = response.json()
                
                self.logger.debug(
                    "api_request_completed",
                    endpoint=endpoint,
                    status_code=response.status_code,
                    response_size=len(response.content)
                )
                
                return data
            
            except ValueError as e:
                self.logger.error(
                    "json_parse_error",
                    endpoint=endpoint,
                    response_text=response.text[:200],
                    error=str(e),
                    error_code=ErrorCode.API_INVALID_RESPONSE
                )
                raise APIResponseError(f"Resposta JSON inválida: {e}")
        
        except requests.Timeout as e:
            self.logger.error(
                "api_timeout",
                endpoint=endpoint,
                timeout=self.config.timeout,
                error=str(e),
                error_code=ErrorCode.API_TIMEOUT
            )
            raise APITimeoutError(f"Timeout na requisição: {e}")
        
        except requests.RequestException as e:
            self.logger.error(
                "api_request_error",
                endpoint=endpoint,
                error=str(e)
            )
            raise APIResponseError(f"Erro na requisição: {e}")
    
    def get_all_positions(self) -> List[Dict]:
        """
        Obtém posições de todos os veículos.
        
        Returns:
            Lista de posições dos veículos
        
        Example:
            >>> client = SPTransAPIClient()
            >>> positions = client.get_all_positions()
            >>> print(f"Total de linhas: {len(positions)}")
        """
        self.logger.info("fetching_all_positions")
        
        data = self._make_request(SPTransEndpoint.POSICAO)
        
        # Extrair lista de linhas
        lines = data.get('l', [])
        
        self.logger.info(
            "positions_fetched",
            total_lines=len(lines),
            timestamp=data.get('hr')
        )
        
        return lines
    
    def get_positions_by_line(self, line_id: int) -> Dict:
        """
        Obtém posições de uma linha específica.
        
        Args:
            line_id: ID da linha
        
        Returns:
            Dados da linha com posições
        """
        self.logger.info("fetching_line_positions", line_id=line_id)
        
        data = self._make_request(
            SPTransEndpoint.POSICAO_LINHA,
            params={'codigoLinha': line_id}
        )
        
        return data
    
    def search_lines(self, search_term: str) -> List[Dict]:
        """
        Busca linhas por termo.
        
        Args:
            search_term: Termo de busca (ex: "8000")
        
        Returns:
            Lista de linhas encontradas
        """
        self.logger.info("searching_lines", search_term=search_term)
        
        lines = self._make_request(
            SPTransEndpoint.LINHAS,
            params={'termosBusca': search_term}
        )
        
        self.logger.info(
            "lines_found",
            search_term=search_term,
            count=len(lines) if isinstance(lines, list) else 0
        )
        
        return lines if isinstance(lines, list) else []
    
    def get_arrival_prediction(self, stop_id: int) -> Dict:
        """
        Obtém previsão de chegada para uma parada.
        
        Args:
            stop_id: ID da parada
        
        Returns:
            Previsões de chegada
        """
        self.logger.info("fetching_arrival_prediction", stop_id=stop_id)
        
        data = self._make_request(
            SPTransEndpoint.PREVISAO_PARADA,
            params={'codigoParada': stop_id}
        )
        
        return data
    
    def close(self) -> None:
        """Fecha a sessão HTTP."""
        self.session.close()
        self.logger.info("session_closed")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


if __name__ == '__main__':
    # Teste do cliente
    print("=== Teste do Cliente API SPTrans ===\n")
    
    try:
        with SPTransAPIClient() as client:
            # Testar autenticação
            print("1. Testando autenticação...")
            client.authenticate()
            print("✓ Autenticação bem-sucedida\n")
            
            # Buscar posições
            print("2. Buscando posições de todos os veículos...")
            positions = client.get_all_positions()
            print(f"✓ Total de linhas encontradas: {len(positions)}\n")
            
            # Mostrar amostra
            if positions:
                first_line = positions[0]
                print("3. Amostra da primeira linha:")
                print(f"   Código: {first_line.get('c')}")
                print(f"   Sentido: {first_line.get('sl')}")
                print(f"   Qtd Veículos: {first_line.get('qv')}")
                
                if first_line.get('vs'):
                    first_vehicle = first_line['vs'][0]
                    print(f"\n   Primeiro veículo:")
                    print(f"   - Prefixo: {first_vehicle.get('p')}")
                    print(f"   - Latitude: {first_vehicle.get('py')}")
                    print(f"   - Longitude: {first_vehicle.get('px')}")
                    print(f"   - Acessível: {first_vehicle.get('a')}")
            
            print("\n✓ Teste concluído com sucesso!")
    
    except Exception as e:
        print(f"\n✗ Erro no teste: {e}")
        import traceback
        traceback.print_exc()
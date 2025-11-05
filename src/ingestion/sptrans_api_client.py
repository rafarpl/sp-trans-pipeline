"""
Cliente para API SPTrans Olho Vivo.

Cliente robusto com retry logic, circuit breaker, rate limiting
e tratamento abrangente de erros.
"""

import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException
from urllib3.util.retry import Retry

from ..common.config import Config
from ..common.constants import (
    API_ENDPOINTS,
    API_MAX_RETRIES,
    API_RATE_LIMIT_PER_MINUTE,
    API_RETRY_BACKOFF_FACTOR,
    API_SUCCESS_CODE,
    API_TIMEOUT_SECONDS,
    SPTRANS_API_BASE_URL,
)
from ..common.exceptions import (
    APIAuthenticationException,
    APIConnectionException,
    APIRateLimitException,
    APIResponseException,
    APITimeoutException,
)
from ..common.logging_config import get_logger
from ..common.metrics import api_requests_total, track_api_call
from ..common.utils import exponential_backoff

logger = get_logger(__name__)


class CircuitBreaker:
    """
    Circuit Breaker para proteção contra falhas em cascata.
    
    Estados:
    - CLOSED: Normal operation
    - OPEN: Bloqueando requests após muitas falhas
    - HALF_OPEN: Testando se serviço recuperou
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
        expected_exception: type = Exception,
    ):
        """
        Inicializa circuit breaker.

        Args:
            failure_threshold: Número de falhas antes de abrir circuito
            recovery_timeout: Tempo em segundos antes de tentar half-open
            expected_exception: Tipo de exceção que conta como falha
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception

        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.state = "CLOSED"

    def call(self, func, *args, **kwargs):
        """
        Executa função com proteção do circuit breaker.

        Args:
            func: Função a executar
            *args, **kwargs: Argumentos da função

        Returns:
            Resultado da função

        Raises:
            Exception: Se circuito estiver OPEN ou função falhar
        """
        if self.state == "OPEN":
            if self._should_attempt_reset():
                self.state = "HALF_OPEN"
                logger.info("Circuit breaker: OPEN → HALF_OPEN")
            else:
                raise Exception("Circuit breaker is OPEN")

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except self.expected_exception as e:
            self._on_failure()
            raise

    def _on_success(self):
        """Registra sucesso."""
        if self.state == "HALF_OPEN":
            self.state = "CLOSED"
            logger.info("Circuit breaker: HALF_OPEN → CLOSED")

        self.failure_count = 0

    def _on_failure(self):
        """Registra falha."""
        self.failure_count += 1
        self.last_failure_time = datetime.now()

        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"
            logger.error(
                f"Circuit breaker: {self.failure_count} failures → OPEN",
                extra={"failure_count": self.failure_count},
            )

    def _should_attempt_reset(self) -> bool:
        """Verifica se deve tentar resetar."""
        if not self.last_failure_time:
            return False

        return (
            datetime.now() - self.last_failure_time
        ).total_seconds() > self.recovery_timeout


class RateLimiter:
    """Rate limiter baseado em sliding window."""

    def __init__(self, max_calls: int, time_window: int):
        """
        Inicializa rate limiter.

        Args:
            max_calls: Número máximo de chamadas
            time_window: Janela de tempo em segundos
        """
        self.max_calls = max_calls
        self.time_window = time_window
        self.calls: List[datetime] = []

    def acquire(self) -> None:
        """
        Aguarda até que seja permitido fazer outra chamada.

        Raises:
            APIRateLimitException: Se rate limit for excedido muito rapidamente
        """
        now = datetime.now()

        # Remove chamadas antigas da janela
        cutoff = now - timedelta(seconds=self.time_window)
        self.calls = [call_time for call_time in self.calls if call_time > cutoff]

        if len(self.calls) >= self.max_calls:
            # Calcular quanto tempo esperar
            oldest_call = self.calls[0]
            wait_time = (oldest_call + timedelta(seconds=self.time_window) - now).total_seconds()

            if wait_time > 0:
                logger.warning(
                    f"Rate limit reached. Waiting {wait_time:.2f}s",
                    extra={"calls_in_window": len(self.calls), "wait_time": wait_time},
                )
                time.sleep(wait_time)

        self.calls.append(datetime.now())


class SPTransAPIClient:
    """
    Cliente robusto para API SPTrans Olho Vivo.
    
    Features:
    - Autenticação automática
    - Retry com exponential backoff
    - Circuit breaker
    - Rate limiting
    - Métricas Prometheus
    - Logging estruturado
    """

    def __init__(self, config: Optional[Config] = None):
        """
        Inicializa cliente API.

        Args:
            config: Configuração (opcional, usa Config() se None)
        """
        self.config = config or Config()
        self.base_url = SPTRANS_API_BASE_URL
        self.api_token = self.config.sptrans.api_token

        # Session com retry automático
        self.session = self._create_session()

        # Cookie de autenticação
        self.auth_cookie: Optional[str] = None
        self.auth_expires_at: Optional[datetime] = None

        # Circuit breaker
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=5, recovery_timeout=60, expected_exception=RequestException
        )

        # Rate limiter
        self.rate_limiter = RateLimiter(
            max_calls=API_RATE_LIMIT_PER_MINUTE, time_window=60
        )

        logger.info(
            "SPTransAPIClient initialized",
            extra={
                "base_url": self.base_url,
                "rate_limit": API_RATE_LIMIT_PER_MINUTE,
            },
        )

    def _create_session(self) -> requests.Session:
        """
        Cria session HTTP com retry automático.

        Returns:
            Session configurada
        """
        session = requests.Session()

        # Configurar retry strategy
        retry_strategy = Retry(
            total=API_MAX_RETRIES,
            backoff_factor=API_RETRY_BACKOFF_FACTOR,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Headers padrão
        session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": "SPTrans-Pipeline/2.0",
            }
        )

        return session

    def authenticate(self, force: bool = False) -> bool:
        """
        Autentica na API SPTrans.

        Args:
            force: Se True, força nova autenticação

        Returns:
            True se autenticado com sucesso

        Raises:
            APIAuthenticationException: Se autenticação falhar
        """
        # Verifica se já está autenticado e não expirou
        if not force and self._is_authenticated():
            logger.debug("Already authenticated, skipping")
            return True

        endpoint = self._get_endpoint_url("authenticate")
        url = f"{endpoint}?token={self.api_token}"

        try:
            logger.info("Authenticating with SPTrans API")
            response = self.session.post(url, timeout=API_TIMEOUT_SECONDS)

            if response.status_code != API_SUCCESS_CODE:
                raise APIAuthenticationException(
                    details={
                        "status_code": response.status_code,
                        "response": response.text[:200],
                    }
                )

            # Verificar resposta
            if not response.json():
                raise APIAuthenticationException(
                    details={"response": "Authentication returned false"}
                )

            # Armazenar cookie de autenticação
            self.auth_cookie = response.cookies.get("apiCredentials")
            self.auth_expires_at = datetime.now() + timedelta(hours=2)

            logger.info(
                "Authentication successful",
                extra={"expires_at": self.auth_expires_at.isoformat()},
            )

            api_requests_total.labels(endpoint="authenticate", status="success").inc()
            return True

        except RequestException as e:
            api_requests_total.labels(endpoint="authenticate", status="error").inc()
            raise APIAuthenticationException(details={"error": str(e)})

    def _is_authenticated(self) -> bool:
        """
        Verifica se está autenticado e token não expirou.

        Returns:
            True se autenticado
        """
        if not self.auth_cookie or not self.auth_expires_at:
            return False

        # Renovar se estiver próximo de expirar (5 min antes)
        if datetime.now() >= (self.auth_expires_at - timedelta(minutes=5)):
            return False

        return True

    def _get_endpoint_url(self, endpoint_key: str) -> str:
        """
        Retorna URL completa do endpoint.

        Args:
            endpoint_key: Chave do endpoint

        Returns:
            URL completa
        """
        if endpoint_key not in API_ENDPOINTS:
            raise ValueError(f"Unknown endpoint: {endpoint_key}")

        return urljoin(self.base_url, API_ENDPOINTS[endpoint_key])

    @track_api_call("positions")
    def get_positions(self, line_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Obtém posições de todos os veículos ou de uma linha específica.

        Args:
            line_id: ID da linha (opcional, None = todas)

        Returns:
            Lista de posições de veículos

        Raises:
            APIException: Se requisição falhar
        """
        self._ensure_authenticated()
        self.rate_limiter.acquire()

        if line_id:
            endpoint = self._get_endpoint_url("positions_by_line")
            url = f"{endpoint}?codigoLinha={line_id}"
        else:
            endpoint = self._get_endpoint_url("positions")
            url = endpoint

        def _request():
            response = self.session.get(url, timeout=API_TIMEOUT_SECONDS)
            response.raise_for_status()
            return response.json()

        try:
            data = self.circuit_breaker.call(_request)
            logger.info(
                f"Retrieved positions",
                extra={"line_id": line_id, "count": len(data.get("l", []))},
            )
            return data

        except RequestException as e:
            self._handle_request_exception(e, "positions")

    def get_lines(self, search_term: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Busca linhas de ônibus.

        Args:
            search_term: Termo de busca (opcional)

        Returns:
            Lista de linhas
        """
        self._ensure_authenticated()
        self.rate_limiter.acquire()

        endpoint = self._get_endpoint_url("lines")
        url = f"{endpoint}?termosBusca={search_term}" if search_term else endpoint

        def _request():
            response = self.session.get(url, timeout=API_TIMEOUT_SECONDS)
            response.raise_for_status()
            return response.json()

        try:
            data = self.circuit_breaker.call(_request)
            logger.info(
                f"Retrieved lines",
                extra={"search_term": search_term, "count": len(data)},
            )
            return data

        except RequestException as e:
            self._handle_request_exception(e, "lines")

    def get_stops(self, search_term: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Busca paradas.

        Args:
            search_term: Termo de busca (opcional)

        Returns:
            Lista de paradas
        """
        self._ensure_authenticated()
        self.rate_limiter.acquire()

        endpoint = self._get_endpoint_url("stops")
        url = f"{endpoint}?termosBusca={search_term}" if search_term else endpoint

        def _request():
            response = self.session.get(url, timeout=API_TIMEOUT_SECONDS)
            response.raise_for_status()
            return response.json()

        try:
            data = self.circuit_breaker.call(_request)
            logger.info(
                f"Retrieved stops",
                extra={"search_term": search_term, "count": len(data)},
            )
            return data

        except RequestException as e:
            self._handle_request_exception(e, "stops")

    def get_corridors(self) -> List[Dict[str, Any]]:
        """
        Obtém lista de corredores.

        Returns:
            Lista de corredores
        """
        self._ensure_authenticated()
        self.rate_limiter.acquire()

        endpoint = self._get_endpoint_url("corridors")

        def _request():
            response = self.session.get(endpoint, timeout=API_TIMEOUT_SECONDS)
            response.raise_for_status()
            return response.json()

        try:
            data = self.circuit_breaker.call(_request)
            logger.info(f"Retrieved corridors", extra={"count": len(data)})
            return data

        except RequestException as e:
            self._handle_request_exception(e, "corridors")

    def _ensure_authenticated(self) -> None:
        """
        Garante que está autenticado antes de fazer requisição.

        Raises:
            APIAuthenticationException: Se autenticação falhar
        """
        if not self._is_authenticated():
            self.authenticate()

    def _handle_request_exception(self, exc: RequestException, endpoint: str) -> None:
        """
        Trata exceções de requisições HTTP.

        Args:
            exc: Exceção capturada
            endpoint: Nome do endpoint

        Raises:
            APIException: Exceção apropriada baseada no erro
        """
        if isinstance(exc, requests.exceptions.Timeout):
            raise APITimeoutException(endpoint=endpoint, timeout=API_TIMEOUT_SECONDS)
        elif isinstance(exc, requests.exceptions.ConnectionError):
            raise APIConnectionException(endpoint=endpoint, reason=str(exc))
        elif isinstance(exc, requests.exceptions.HTTPError):
            status_code = exc.response.status_code if exc.response else 0
            response_body = exc.response.text if exc.response else ""

            if status_code == 429:
                raise APIRateLimitException()
            elif status_code == 401:
                # Token pode ter expirado, tentar re-autenticar
                self.authenticate(force=True)
                raise APIAuthenticationException(
                    details={"reason": "Token expired, re-authenticate"}
                )
            else:
                raise APIResponseException(
                    status_code=status_code,
                    response_body=response_body,
                    endpoint=endpoint,
                )
        else:
            raise APIResponseException(
                status_code=0, response_body=str(exc), endpoint=endpoint
            )

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.session.close()


# Exemplo de uso
if __name__ == "__main__":
    from ..common.config import Config
    from ..common.logging_config import setup_logging

    # Setup logging
    setup_logging(log_level="INFO", log_format="console")

    # Criar cliente
    client = SPTransAPIClient()

    # Usar como context manager
    with client:
        # Autenticar
        client.authenticate()

        # Buscar posições
        positions = client.get_positions()
        print(f"Total vehicles: {len(positions.get('l', []))}")

        # Buscar linhas
        lines = client.get_lines(search_term="8000")
        print(f"Lines found: {len(lines)}")

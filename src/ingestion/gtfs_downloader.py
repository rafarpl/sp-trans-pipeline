"""
GTFS Downloader e Parser para SPTrans.

Download, extração e parse de arquivos GTFS estáticos
do sistema de transporte de São Paulo.
"""

import csv
import io
import shutil
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests

from ..common.config import Config
from ..common.constants import SPTRANS_GTFS_URL
from ..common.exceptions import (
    APIConnectionException,
    APITimeoutException,
    StorageReadException,
    StorageWriteException,
)
from ..common.logging_config import get_logger
from ..common.utils import ensure_dir, get_file_size

logger = get_logger(__name__)

# Arquivos GTFS suportados
GTFS_FILES = [
    "agency.txt",
    "stops.txt",
    "routes.txt",
    "trips.txt",
    "stop_times.txt",
    "calendar.txt",
    "calendar_dates.txt",
    "shapes.txt",
    "frequencies.txt",
    "feed_info.txt",
]


class GTFSDownloader:
    """
    Downloader e parser para arquivos GTFS.
    
    Faz download do feed GTFS da SPTrans, extrai os arquivos
    e fornece métodos para parse dos dados.
    """

    def __init__(
        self,
        config: Optional[Config] = None,
        download_url: str = SPTRANS_GTFS_URL,
        cache_dir: Optional[Path] = None,
    ):
        """
        Inicializa downloader.

        Args:
            config: Configuração (opcional)
            download_url: URL do feed GTFS
            cache_dir: Diretório para cache (opcional)
        """
        self.config = config or Config()
        self.download_url = download_url
        self.cache_dir = cache_dir or Path("/tmp/gtfs_cache")

        ensure_dir(self.cache_dir)

        logger.info(
            "GTFSDownloader initialized",
            extra={
                "download_url": self.download_url,
                "cache_dir": str(self.cache_dir),
            },
        )

    def download_gtfs(
        self, force: bool = False, timeout: int = 300
    ) -> Path:
        """
        Faz download do arquivo GTFS zip.

        Args:
            force: Se True, força novo download mesmo se já existir
            timeout: Timeout em segundos

        Returns:
            Path do arquivo zip baixado

        Raises:
            APIConnectionException: Se download falhar
            StorageWriteException: Se não conseguir salvar arquivo
        """
        zip_path = self.cache_dir / "gtfs.zip"

        # Se já existe e não é para forçar, retorna
        if zip_path.exists() and not force:
            logger.info(
                "GTFS file already exists, skipping download",
                extra={"path": str(zip_path), "size_bytes": get_file_size(zip_path)},
            )
            return zip_path

        logger.info(f"Downloading GTFS from {self.download_url}")

        try:
            response = requests.get(
                self.download_url, timeout=timeout, stream=True
            )
            response.raise_for_status()

            # Salvar arquivo
            with open(zip_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            file_size = get_file_size(zip_path)
            logger.info(
                "GTFS downloaded successfully",
                extra={"path": str(zip_path), "size_bytes": file_size},
            )

            return zip_path

        except requests.exceptions.Timeout:
            raise APITimeoutException(endpoint=self.download_url, timeout=timeout)
        except requests.exceptions.RequestException as e:
            raise APIConnectionException(endpoint=self.download_url, reason=str(e))
        except IOError as e:
            raise StorageWriteException(location=str(zip_path), reason=str(e))

    def extract_gtfs(self, zip_path: Optional[Path] = None) -> Path:
        """
        Extrai arquivos GTFS do zip.

        Args:
            zip_path: Path do arquivo zip (opcional, usa cache se None)

        Returns:
            Path do diretório com arquivos extraídos

        Raises:
            StorageReadException: Se não conseguir ler/extrair zip
        """
        if zip_path is None:
            zip_path = self.cache_dir / "gtfs.zip"

        extract_dir = self.cache_dir / "gtfs_extracted"

        # Se já existe, limpar
        if extract_dir.exists():
            shutil.rmtree(extract_dir)

        ensure_dir(extract_dir)

        try:
            logger.info(f"Extracting GTFS to {extract_dir}")

            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(extract_dir)

            # Contar arquivos extraídos
            extracted_files = list(extract_dir.glob("*.txt"))
            logger.info(
                "GTFS extracted successfully",
                extra={
                    "extract_dir": str(extract_dir),
                    "files_count": len(extracted_files),
                },
            )

            return extract_dir

        except zipfile.BadZipFile as e:
            raise StorageReadException(location=str(zip_path), reason=f"Invalid zip: {e}")
        except IOError as e:
            raise StorageReadException(location=str(zip_path), reason=str(e))

    def parse_gtfs_file(
        self, filename: str, extract_dir: Optional[Path] = None
    ) -> List[Dict[str, Any]]:
        """
        Parse um arquivo GTFS específico.

        Args:
            filename: Nome do arquivo (ex: 'stops.txt')
            extract_dir: Diretório com arquivos extraídos (opcional)

        Returns:
            Lista de dicionários com dados

        Raises:
            StorageReadException: Se arquivo não existir ou erro ao ler
        """
        if extract_dir is None:
            extract_dir = self.cache_dir / "gtfs_extracted"

        file_path = extract_dir / filename

        if not file_path.exists():
            raise StorageReadException(
                location=str(file_path), reason="File not found"
            )

        try:
            logger.info(f"Parsing GTFS file: {filename}")

            with open(file_path, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                data = list(reader)

            logger.info(
                f"Parsed {filename}",
                extra={"filename": filename, "records": len(data)},
            )

            return data

        except Exception as e:
            raise StorageReadException(location=str(file_path), reason=str(e))

    def parse_stops(self, extract_dir: Optional[Path] = None) -> List[Dict[str, Any]]:
        """
        Parse arquivo stops.txt.

        Args:
            extract_dir: Diretório com arquivos extraídos (opcional)

        Returns:
            Lista de paradas
        """
        return self.parse_gtfs_file("stops.txt", extract_dir)

    def parse_routes(self, extract_dir: Optional[Path] = None) -> List[Dict[str, Any]]:
        """
        Parse arquivo routes.txt.

        Args:
            extract_dir: Diretório com arquivos extraídos (opcional)

        Returns:
            Lista de rotas
        """
        return self.parse_gtfs_file("routes.txt", extract_dir)

    def parse_trips(self, extract_dir: Optional[Path] = None) -> List[Dict[str, Any]]:
        """
        Parse arquivo trips.txt.

        Args:
            extract_dir: Diretório com arquivos extraídos (opcional)

        Returns:
            Lista de viagens
        """
        return self.parse_gtfs_file("trips.txt", extract_dir)

    def parse_stop_times(
        self, extract_dir: Optional[Path] = None
    ) -> List[Dict[str, Any]]:
        """
        Parse arquivo stop_times.txt.

        Args:
            extract_dir: Diretório com arquivos extraídos (opcional)

        Returns:
            Lista de horários de parada
        """
        return self.parse_gtfs_file("stop_times.txt", extract_dir)

    def parse_shapes(self, extract_dir: Optional[Path] = None) -> List[Dict[str, Any]]:
        """
        Parse arquivo shapes.txt.

        Args:
            extract_dir: Diretório com arquivos extraídos (opcional)

        Returns:
            Lista de formas/traçados
        """
        return self.parse_gtfs_file("shapes.txt", extract_dir)

    def parse_all(self, extract_dir: Optional[Path] = None) -> Dict[str, List[Dict[str, Any]]]:
        """
        Parse todos os arquivos GTFS disponíveis.

        Args:
            extract_dir: Diretório com arquivos extraídos (opcional)

        Returns:
            Dicionário {filename: data}
        """
        if extract_dir is None:
            extract_dir = self.cache_dir / "gtfs_extracted"

        results = {}

        for filename in GTFS_FILES:
            file_path = extract_dir / filename

            if not file_path.exists():
                logger.warning(f"GTFS file not found, skipping: {filename}")
                continue

            try:
                results[filename] = self.parse_gtfs_file(filename, extract_dir)
            except Exception as e:
                logger.error(
                    f"Error parsing {filename}: {e}",
                    extra={"filename": filename, "error": str(e)},
                )
                continue

        logger.info(
            "Parsed all GTFS files",
            extra={"files_parsed": len(results), "total_files": len(GTFS_FILES)},
        )

        return results

    def download_and_parse(
        self, force_download: bool = False
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Faz download, extração e parse completo do GTFS.

        Args:
            force_download: Se True, força novo download

        Returns:
            Dicionário {filename: data}
        """
        # Download
        zip_path = self.download_gtfs(force=force_download)

        # Extração
        extract_dir = self.extract_gtfs(zip_path)

        # Parse
        data = self.parse_all(extract_dir)

        return data

    def get_stats(
        self, data: Optional[Dict[str, List[Dict[str, Any]]]] = None
    ) -> Dict[str, int]:
        """
        Retorna estatísticas dos dados GTFS.

        Args:
            data: Dados parseados (opcional, parse se None)

        Returns:
            Dicionário com contagens
        """
        if data is None:
            data = self.parse_all()

        stats = {}
        for filename, records in data.items():
            key = filename.replace(".txt", "")
            stats[key] = len(records)

        logger.info("GTFS stats", extra=stats)
        return stats

    def validate_gtfs(
        self, data: Optional[Dict[str, List[Dict[str, Any]]]] = None
    ) -> Dict[str, Any]:
        """
        Valida dados GTFS básicos.

        Args:
            data: Dados parseados (opcional)

        Returns:
            Dicionário com resultados de validação
        """
        if data is None:
            data = self.parse_all()

        validation = {
            "is_valid": True,
            "errors": [],
            "warnings": [],
            "stats": self.get_stats(data),
        }

        # Arquivos obrigatórios
        required_files = ["stops.txt", "routes.txt", "trips.txt", "stop_times.txt"]

        for req_file in required_files:
            if req_file not in data:
                validation["is_valid"] = False
                validation["errors"].append(f"Missing required file: {req_file}")

        # Validar que não estão vazios
        for filename, records in data.items():
            if len(records) == 0:
                validation["warnings"].append(f"Empty file: {filename}")

        logger.info(
            "GTFS validation complete",
            extra={
                "is_valid": validation["is_valid"],
                "errors_count": len(validation["errors"]),
                "warnings_count": len(validation["warnings"]),
            },
        )

        return validation


# Exemplo de uso
if __name__ == "__main__":
    from ..common.logging_config import setup_logging

    setup_logging(log_level="INFO", log_format="console")

    # Criar downloader
    downloader = GTFSDownloader()

    # Download e parse completo
    data = downloader.download_and_parse(force_download=False)

    # Stats
    stats = downloader.get_stats(data)
    print(f"\nGTFS Stats:")
    for key, count in stats.items():
        print(f"  {key}: {count:,} records")

    # Validação
    validation = downloader.validate_gtfs(data)
    print(f"\nValidation:")
    print(f"  Valid: {validation['is_valid']}")
    print(f"  Errors: {len(validation['errors'])}")
    print(f"  Warnings: {len(validation['warnings'])}")

"""
GTFS Downloader Module
======================
Download e extração de arquivos GTFS da SPTrans.

GTFS (General Transit Feed Specification) contém dados estáticos sobre:
- routes: Linhas de ônibus
- trips: Viagens programadas
- stops: Pontos de parada
- stop_times: Horários de chegada/saída
- shapes: Traçado geográfico das rotas
"""

import os
import zipfile
import requests
from typing import List, Optional, Dict, Any
from datetime import datetime
from pathlib import Path
import shutil

from src.common.logging_config import get_logger
from src.common.exceptions import DownloadError
from src.common.metrics import track_api_call, metrics

logger = get_logger(__name__)


class GTFSDownloader:
    """Cliente para download de dados GTFS da SPTrans."""
    
    # URL oficial do GTFS da SPTrans
    DEFAULT_GTFS_URL = "https://www.sptrans.com.br/umbraco/surface/PerfilDesenvolvedor/BaixarGTFS"
    
    # Arquivos esperados no GTFS
    REQUIRED_FILES = [
        'routes.txt',
        'trips.txt',
        'stops.txt',
        'stop_times.txt',
        'shapes.txt',
        'calendar.txt',
        'agency.txt'
    ]
    
    OPTIONAL_FILES = [
        'calendar_dates.txt',
        'fare_attributes.txt',
        'fare_rules.txt',
        'frequencies.txt',
        'transfers.txt'
    ]
    
    def __init__(self, 
                 download_dir: str = './data/gtfs',
                 gtfs_url: Optional[str] = None):
        """
        Inicializa downloader.
        
        Args:
            download_dir: Diretório para salvar arquivos
            gtfs_url: URL customizada do GTFS (usa default se None)
        """
        self.download_dir = Path(download_dir)
        self.gtfs_url = gtfs_url or self.DEFAULT_GTFS_URL
        
        # Criar diretório se não existir
        self.download_dir.mkdir(parents=True, exist_ok=True)
        
        # Configurar sessão HTTP
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'SPTransPipeline/1.0'
        })
        
        logger.info(f"GTFSDownloader inicializado: dir={download_dir}, url={self.gtfs_url}")
    
    @track_api_call('gtfs_download')
    def download(self, output_filename: Optional[str] = None) -> str:
        """
        Baixa arquivo ZIP do GTFS.
        
        Args:
            output_filename: Nome do arquivo de saída (gera automático se None)
        
        Returns:
            Caminho do arquivo baixado
        
        Raises:
            DownloadError: Se o download falhar
        """
        if output_filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_filename = f'gtfs_{timestamp}.zip'
        
        output_path = self.download_dir / output_filename
        
        logger.info(f"Iniciando download do GTFS de {self.gtfs_url}")
        
        try:
            # Download com streaming para não sobrecarregar memória
            response = self.session.get(self.gtfs_url, stream=True, timeout=300)
            response.raise_for_status()
            
            # Obter tamanho do arquivo
            total_size = int(response.headers.get('content-length', 0))
            logger.info(f"Tamanho do arquivo: {total_size / 1024 / 1024:.2f} MB")
            
            # Download com progresso
            downloaded_size = 0
            chunk_size = 8192
            
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        downloaded_size += len(chunk)
                        
                        # Log de progresso a cada 10%
                        if total_size > 0:
                            progress = (downloaded_size / total_size) * 100
                            if int(progress) % 10 == 0:
                                logger.info(f"Progresso: {progress:.0f}%")
            
            logger.info(f"Download concluído: {output_path}")
            
            # Validar arquivo ZIP
            if not zipfile.is_zipfile(output_path):
                raise DownloadError(f"Arquivo baixado não é um ZIP válido: {output_path}")
            
            # Reportar métricas
            metrics.storage_size.labels(
                layer='gtfs_raw',
                format='zip'
            ).set(output_path.stat().st_size)
            
            return str(output_path)
        
        except requests.RequestException as e:
            error_msg = f"Erro ao baixar GTFS: {e}"
            logger.error(error_msg)
            raise DownloadError(error_msg) from e
        
        except Exception as e:
            error_msg = f"Erro inesperado no download: {e}"
            logger.error(error_msg)
            raise DownloadError(error_msg) from e
    
    def extract(self, zip_path: str, extract_dir: Optional[str] = None) -> List[str]:
        """
        Extrai arquivos do ZIP.
        
        Args:
            zip_path: Caminho do arquivo ZIP
            extract_dir: Diretório de extração (usa download_dir se None)
        
        Returns:
            Lista de arquivos extraídos
        
        Raises:
            DownloadError: Se a extração falhar
        """
        if extract_dir is None:
            extract_dir = self.download_dir
        else:
            extract_dir = Path(extract_dir)
        
        extract_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Extraindo {zip_path} para {extract_dir}")
        
        try:
            extracted_files = []
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Listar arquivos no ZIP
                file_list = zip_ref.namelist()
                logger.info(f"Arquivos no ZIP: {len(file_list)}")
                
                # Extrair todos os arquivos
                for file_name in file_list:
                    # Extrair apenas arquivos .txt (ignorar diretórios e outros arquivos)
                    if file_name.endswith('.txt'):
                        zip_ref.extract(file_name, extract_dir)
                        extracted_path = extract_dir / file_name
                        extracted_files.append(file_name)
                        
                        # Log do tamanho
                        size_mb = extracted_path.stat().st_size / 1024 / 1024
                        logger.info(f"Extraído: {file_name} ({size_mb:.2f} MB)")
            
            logger.info(f"Extração concluída: {len(extracted_files)} arquivos")
            
            # Validar arquivos obrigatórios
            self._validate_extracted_files(extracted_files)
            
            return extracted_files
        
        except zipfile.BadZipFile as e:
            error_msg = f"Arquivo ZIP corrompido: {e}"
            logger.error(error_msg)
            raise DownloadError(error_msg) from e
        
        except Exception as e:
            error_msg = f"Erro ao extrair ZIP: {e}"
            logger.error(error_msg)
            raise DownloadError(error_msg) from e
    
    def _validate_extracted_files(self, extracted_files: List[str]):
        """
        Valida se todos os arquivos obrigatórios foram extraídos.
        
        Args:
            extracted_files: Lista de arquivos extraídos
        
        Raises:
            DownloadError: Se arquivos obrigatórios estiverem faltando
        """
        missing_files = [
            f for f in self.REQUIRED_FILES 
            if f not in extracted_files
        ]
        
        if missing_files:
            error_msg = f"Arquivos obrigatórios faltando: {missing_files}"
            logger.error(error_msg)
            raise DownloadError(error_msg)
        
        logger.info("Todos os arquivos obrigatórios presentes ✓")
        
        # Log de arquivos opcionais presentes
        optional_present = [
            f for f in self.OPTIONAL_FILES 
            if f in extracted_files
        ]
        if optional_present:
            logger.info(f"Arquivos opcionais presentes: {optional_present}")
    
    def download_and_extract(self, cleanup_zip: bool = True) -> Dict[str, Any]:
        """
        Download e extração em um único método.
        
        Args:
            cleanup_zip: Se deve remover o ZIP após extração
        
        Returns:
            Dict com informações do download
        """
        logger.info("Iniciando download e extração do GTFS")
        
        # Download
        start_time = datetime.now()
        zip_path = self.download()
        download_duration = (datetime.now() - start_time).total_seconds()
        
        # Extração
        start_time = datetime.now()
        extracted_files = self.extract(zip_path)
        extract_duration = (datetime.now() - start_time).total_seconds()
        
        # Cleanup
        if cleanup_zip:
            logger.info(f"Removendo arquivo ZIP: {zip_path}")
            os.remove(zip_path)
        
        result = {
            'success': True,
            'download_duration_seconds': download_duration,
            'extract_duration_seconds': extract_duration,
            'total_duration_seconds': download_duration + extract_duration,
            'extracted_files': extracted_files,
            'file_count': len(extracted_files),
            'extract_dir': str(self.download_dir),
            'timestamp': datetime.now().isoformat()
        }
        
        logger.info(f"Download e extração concluídos: {result}")
        
        return result
    
    def get_file_path(self, filename: str) -> str:
        """
        Retorna caminho completo de um arquivo GTFS.
        
        Args:
            filename: Nome do arquivo (ex: 'routes.txt')
        
        Returns:
            Caminho completo do arquivo
        """
        return str(self.download_dir / filename)
    
    def validate_gtfs_integrity(self) -> Dict[str, Any]:
        """
        Valida integridade dos arquivos GTFS extraídos.
        
        Returns:
            Dict com estatísticas de validação
        """
        logger.info("Validando integridade do GTFS")
        
        validation = {
            'required_files': {},
            'optional_files': {},
            'errors': [],
            'warnings': []
        }
        
        # Verificar arquivos obrigatórios
        for filename in self.REQUIRED_FILES:
            filepath = self.download_dir / filename
            
            if filepath.exists():
                size = filepath.stat().st_size
                # Contar linhas (excluindo header)
                with open(filepath, 'r', encoding='utf-8') as f:
                    line_count = sum(1 for _ in f) - 1
                
                validation['required_files'][filename] = {
                    'exists': True,
                    'size_bytes': size,
                    'size_mb': round(size / 1024 / 1024, 2),
                    'line_count': line_count
                }
                
                # Validação básica
                if size == 0:
                    validation['errors'].append(f"{filename} está vazio")
                elif line_count == 0:
                    validation['warnings'].append(f"{filename} não tem dados")
            else:
                validation['required_files'][filename] = {'exists': False}
                validation['errors'].append(f"{filename} não encontrado")
        
        # Verificar arquivos opcionais
        for filename in self.OPTIONAL_FILES:
            filepath = self.download_dir / filename
            
            if filepath.exists():
                size = filepath.stat().st_size
                validation['optional_files'][filename] = {
                    'exists': True,
                    'size_mb': round(size / 1024 / 1024, 2)
                }
        
        # Status geral
        validation['is_valid'] = len(validation['errors']) == 0
        validation['completeness'] = (
            len([f for f in validation['required_files'].values() if f.get('exists')]) / 
            len(self.REQUIRED_FILES) * 100
        )
        
        logger.info(f"Validação: {validation['completeness']:.1f}% completo, "
                   f"{len(validation['errors'])} erros, "
                   f"{len(validation['warnings'])} avisos")
        
        return validation
    
    def cleanup_old_files(self, keep_days: int = 7):
        """
        Remove arquivos GTFS antigos.
        
        Args:
            keep_days: Manter arquivos dos últimos N dias
        """
        logger.info(f"Limpando arquivos GTFS com mais de {keep_days} dias")
        
        cutoff_time = datetime.now().timestamp() - (keep_days * 24 * 60 * 60)
        removed_count = 0
        
        for filepath in self.download_dir.glob('*.txt'):
            if filepath.stat().st_mtime < cutoff_time:
                logger.info(f"Removendo arquivo antigo: {filepath}")
                filepath.unlink()
                removed_count += 1
        
        for filepath in self.download_dir.glob('*.zip'):
            if filepath.stat().st_mtime < cutoff_time:
                logger.info(f"Removendo ZIP antigo: {filepath}")
                filepath.unlink()
                removed_count += 1
        
        logger.info(f"Cleanup concluído: {removed_count} arquivos removidos")


def download_gtfs(output_dir: str = './data/gtfs', 
                  cleanup_zip: bool = True) -> Dict[str, Any]:
    """
    Função auxiliar para download rápido do GTFS.
    
    Args:
        output_dir: Diretório de saída
        cleanup_zip: Remover ZIP após extração
    
    Returns:
        Dict com informações do download
    """
    downloader = GTFSDownloader(download_dir=output_dir)
    return downloader.download_and_extract(cleanup_zip=cleanup_zip)


if __name__ == "__main__":
    # Teste do módulo
    print("=== Testando GTFSDownloader ===\n")
    
    # Criar downloader
    downloader = GTFSDownloader(download_dir='./data/gtfs_test')
    
    print("1. Download e extração do GTFS...")
    try:
        result = downloader.download_and_extract(cleanup_zip=False)
        print(f"   ✅ Sucesso: {result['file_count']} arquivos extraídos")
        print(f"   ⏱️  Duração: {result['total_duration_seconds']:.2f}s")
    except Exception as e:
        print(f"   ❌ Erro: {e}")
    
    print("\n2. Validando integridade...")
    validation = downloader.validate_gtfs_integrity()
    print(f"   Completude: {validation['completeness']:.1f}%")
    print(f"   Erros: {len(validation['errors'])}")
    print(f"   Avisos: {len(validation['warnings'])}")
    
    if validation['errors']:
        print(f"   ❌ Erros encontrados: {validation['errors']}")
    else:
        print("   ✅ GTFS válido")
    
    print("\n✅ Testes concluídos!")

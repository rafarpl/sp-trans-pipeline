"""
Testes Unitários: Funções Utilitárias
Projeto: SPTrans Real-Time Data Pipeline
Autor: Equipe LABDATA/FIA

Testa as funções utilitárias do projeto:
- Formatação de dados
- Conversões
- Helpers
- Validadores
"""

import pytest
import json
from datetime import datetime, timedelta
from pathlib import Path
import tempfile
import os


class TestDateTimeUtils:
    """Testes para utilitários de data/hora"""
    
    def test_format_timestamp(self):
        """Testa formatação de timestamp"""
        dt = datetime(2025, 10, 29, 14, 30, 45)
        
        # Formato ISO
        iso_format = dt.isoformat()
        assert iso_format == "2025-10-29T14:30:45"
        
        # Formato brasileiro
        br_format = dt.strftime("%d/%m/%Y %H:%M:%S")
        assert br_format == "29/10/2025 14:30:45"
    
    def test_parse_multiple_timestamp_formats(self):
        """Testa parsing de múltiplos formatos de timestamp"""
        formats = [
            ("2025-10-29 14:30:45", "%Y-%m-%d %H:%M:%S"),
            ("2025-10-29T14:30:45", "%Y-%m-%dT%H:%M:%S"),
            ("29/10/2025 14:30:45", "%d/%m/%Y %H:%M:%S"),
            ("20251029143045", "%Y%m%d%H%M%S"),
        ]
        
        for timestamp_str, fmt in formats:
            parsed = datetime.strptime(timestamp_str, fmt)
            assert parsed.year == 2025
            assert parsed.month == 10
            assert parsed.day == 29
    
    def test_calculate_time_difference(self):
        """Testa cálculo de diferença de tempo"""
        start = datetime(2025, 10, 29, 8, 0, 0)
        end = datetime(2025, 10, 29, 10, 30, 0)
        
        diff = end - start
        
        assert diff.total_seconds() == 9000  # 2h30min = 9000s
        assert diff.seconds // 3600 == 2  # 2 horas
        assert (diff.seconds % 3600) // 60 == 30  # 30 minutos
    
    def test_add_time_delta(self):
        """Testa adição de timedelta"""
        dt = datetime(2025, 10, 29, 8, 0, 0)
        
        # Adicionar 2 horas
        new_dt = dt + timedelta(hours=2)
        assert new_dt.hour == 10
        
        # Adicionar 30 minutos
        new_dt = dt + timedelta(minutes=30)
        assert new_dt.minute == 30
        
        # Adicionar 1 dia
        new_dt = dt + timedelta(days=1)
        assert new_dt.day == 30
    
    def test_get_current_timestamp(self):
        """Testa obtenção de timestamp atual"""
        now = datetime.now()
        
        assert isinstance(now, datetime)
        assert now.year >= 2025
        
        # Formato Unix timestamp
        unix_ts = now.timestamp()
        assert unix_ts > 0


class TestStringUtils:
    """Testes para utilitários de string"""
    
    def test_normalize_string(self):
        """Testa normalização de strings"""
        test_cases = [
            ("  hello  ", "hello"),
            ("HELLO", "hello"),
            ("  HELLO  ", "hello"),
            ("hello\n", "hello"),
            ("\tHELLO\t", "hello"),
        ]
        
        for input_str, expected in test_cases:
            normalized = input_str.strip().lower()
            assert normalized == expected
    
    def test_remove_special_characters(self):
        """Testa remoção de caracteres especiais"""
        import re
        
        test_string = "Hello@World#2025!"
        cleaned = re.sub(r'[^a-zA-Z0-9\s]', '', test_string)
        
        assert cleaned == "HelloWorld2025"
        assert "@" not in cleaned
        assert "#" not in cleaned
    
    def test_slugify(self):
        """Testa conversão de string para slug"""
        import re
        
        def slugify(text):
            text = text.lower().strip()
            text = re.sub(r'[^\w\s-]', '', text)
            text = re.sub(r'[\s_-]+', '-', text)
            text = re.sub(r'^-+|-+$', '', text)
            return text
        
        assert slugify("Hello World") == "hello-world"
        assert slugify("São Paulo") == "so-paulo"
        assert slugify("  Multiple   Spaces  ") == "multiple-spaces"
    
    def test_truncate_string(self):
        """Testa truncamento de string"""
        long_string = "A" * 100
        
        truncated = long_string[:50]
        assert len(truncated) == 50
        
        # Com ellipsis
        max_length = 50
        truncated_with_ellipsis = (long_string[:max_length-3] + "...") if len(long_string) > max_length else long_string
        assert len(truncated_with_ellipsis) == 50
        assert truncated_with_ellipsis.endswith("...")


class TestNumberUtils:
    """Testes para utilitários numéricos"""
    
    def test_round_to_decimals(self):
        """Testa arredondamento para casas decimais"""
        value = 3.14159265359
        
        assert round(value, 2) == 3.14
        assert round(value, 4) == 3.1416
        assert round(value, 0) == 3.0
    
    def test_format_number_with_thousands_separator(self):
        """Testa formatação de números com separador de milhares"""
        value = 1234567.89
        
        formatted = f"{value:,.2f}"
        assert formatted == "1,234,567.89"
        
        # Formato brasileiro
        formatted_br = f"{value:_.2f}".replace(".", ",").replace("_", ".")
        assert "1.234.567" in formatted_br
    
    def test_percentage_calculation(self):
        """Testa cálculo de porcentagem"""
        part = 25
        total = 100
        
        percentage = (part / total) * 100
        assert percentage == 25.0
        
        # Formato com 2 casas decimais
        formatted = f"{percentage:.2f}%"
        assert formatted == "25.00%"
    
    def test_clamp_value(self):
        """Testa limitação de valor entre min e max"""
        def clamp(value, min_val, max_val):
            return max(min_val, min(value, max_val))
        
        assert clamp(5, 0, 10) == 5
        assert clamp(-5, 0, 10) == 0
        assert clamp(15, 0, 10) == 10


class TestFileUtils:
    """Testes para utilitários de arquivo"""
    
    def test_read_json_file(self):
        """Testa leitura de arquivo JSON"""
        test_data = {"key": "value", "number": 123}
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(test_data, f)
            temp_path = f.name
        
        try:
            with open(temp_path, 'r') as f:
                loaded_data = json.load(f)
            
            assert loaded_data == test_data
            assert loaded_data["key"] == "value"
            assert loaded_data["number"] == 123
        finally:
            os.unlink(temp_path)
    
    def test_write_json_file(self):
        """Testa escrita de arquivo JSON"""
        test_data = {"name": "Test", "values": [1, 2, 3]}
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(test_data, f, indent=2)
            temp_path = f.name
        
        try:
            with open(temp_path, 'r') as f:
                content = f.read()
            
            assert "name" in content
            assert "Test" in content
            assert "values" in content
        finally:
            os.unlink(temp_path)
    
    def test_check_file_exists(self):
        """Testa verificação de existência de arquivo"""
        # Criar arquivo temporário
        with tempfile.NamedTemporaryFile(delete=False) as f:
            temp_path = f.name
        
        try:
            assert os.path.exists(temp_path)
            assert os.path.isfile(temp_path)
        finally:
            os.unlink(temp_path)
        
        # Verificar que arquivo foi deletado
        assert not os.path.exists(temp_path)
    
    def test_create_directory(self):
        """Testa criação de diretório"""
        temp_dir = tempfile.mkdtemp()
        
        try:
            assert os.path.exists(temp_dir)
            assert os.path.isdir(temp_dir)
            
            # Criar subdiretório
            sub_dir = os.path.join(temp_dir, "subdir")
            os.makedirs(sub_dir, exist_ok=True)
            
            assert os.path.exists(sub_dir)
        finally:
            # Limpar
            import shutil
            shutil.rmtree(temp_dir)
    
    def test_get_file_size(self):
        """Testa obtenção de tamanho de arquivo"""
        test_content = "Hello World" * 100
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            f.write(test_content)
            temp_path = f.name
        
        try:
            file_size = os.path.getsize(temp_path)
            assert file_size > 0
            assert file_size == len(test_content)
        finally:
            os.unlink(temp_path)


class TestValidationUtils:
    """Testes para utilitários de validação"""
    
    def test_validate_email(self):
        """Testa validação de email"""
        import re
        
        email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
        
        valid_emails = [
            "user@example.com",
            "test.user@example.com",
            "user+tag@example.co.uk"
        ]
        
        invalid_emails = [
            "notanemail",
            "@example.com",
            "user@",
            "user@example"
        ]
        
        for email in valid_emails:
            assert email_pattern.match(email), f"{email} deveria ser válido"
        
        for email in invalid_emails:
            assert not email_pattern.match(email), f"{email} deveria ser inválido"
    
    def test_validate_url(self):
        """Testa validação de URL"""
        from urllib.parse import urlparse
        
        def is_valid_url(url):
            try:
                result = urlparse(url)
                return all([result.scheme, result.netloc])
            except:
                return False
        
        valid_urls = [
            "http://example.com",
            "https://example.com",
            "https://example.com/path",
            "https://example.com/path?query=1"
        ]
        
        invalid_urls = [
            "not a url",
            "example.com",
            "://example.com"
        ]
        
        for url in valid_urls:
            assert is_valid_url(url), f"{url} deveria ser válido"
        
        for url in invalid_urls:
            assert not is_valid_url(url), f"{url} deveria ser inválido"
    
    def test_validate_range(self):
        """Testa validação de range de valores"""
        def is_in_range(value, min_val, max_val):
            return min_val <= value <= max_val
        
        assert is_in_range(5, 0, 10)
        assert is_in_range(0, 0, 10)
        assert is_in_range(10, 0, 10)
        assert not is_in_range(-1, 0, 10)
        assert not is_in_range(11, 0, 10)


class TestConversionUtils:
    """Testes para utilitários de conversão"""
    
    def test_convert_km_to_miles(self):
        """Testa conversão de km para milhas"""
        km = 10
        miles = km * 0.621371
        
        assert miles == pytest.approx(6.21371, 0.001)
    
    def test_convert_celsius_to_fahrenheit(self):
        """Testa conversão de Celsius para Fahrenheit"""
        celsius = 25
        fahrenheit = (celsius * 9/5) + 32
        
        assert fahrenheit == 77.0
    
    def test_convert_bytes_to_human_readable(self):
        """Testa conversão de bytes para formato legível"""
        def format_bytes(bytes_value):
            for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
                if bytes_value < 1024.0:
                    return f"{bytes_value:.2f} {unit}"
                bytes_value /= 1024.0
            return f"{bytes_value:.2f} PB"
        
        assert format_bytes(1024) == "1.00 KB"
        assert format_bytes(1024 * 1024) == "1.00 MB"
        assert format_bytes(1024 * 1024 * 1024) == "1.00 GB"


class TestCollectionUtils:
    """Testes para utilitários de coleções"""
    
    def test_chunk_list(self):
        """Testa divisão de lista em chunks"""
        def chunk_list(lst, chunk_size):
            return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]
        
        data = list(range(10))
        chunks = chunk_list(data, 3)
        
        assert len(chunks) == 4
        assert chunks[0] == [0, 1, 2]
        assert chunks[-1] == [9]
    
    def test_flatten_list(self):
        """Testa achatamento de lista aninhada"""
        nested = [[1, 2], [3, 4], [5, 6]]
        flattened = [item for sublist in nested for item in sublist]
        
        assert flattened == [1, 2, 3, 4, 5, 6]
    
    def test_remove_duplicates(self):
        """Testa remoção de duplicatas"""
        data = [1, 2, 2, 3, 3, 3, 4, 5, 5]
        unique = list(dict.fromkeys(data))  # Mantém ordem
        
        assert unique == [1, 2, 3, 4, 5]
        assert len(unique) == 5
    
    def test_merge_dictionaries(self):
        """Testa merge de dicionários"""
        dict1 = {"a": 1, "b": 2}
        dict2 = {"b": 3, "c": 4}
        
        merged = {**dict1, **dict2}
        
        assert merged == {"a": 1, "b": 3, "c": 4}
        assert merged["b"] == 3  # Segundo dict sobrescreve


class TestHashingUtils:
    """Testes para utilitários de hash"""
    
    def test_generate_md5_hash(self):
        """Testa geração de hash MD5"""
        import hashlib
        
        text = "Hello World"
        md5_hash = hashlib.md5(text.encode()).hexdigest()
        
        assert len(md5_hash) == 32
        assert md5_hash == "b10a8db164e0754105b7a99be72e3fe5"
    
    def test_generate_sha256_hash(self):
        """Testa geração de hash SHA256"""
        import hashlib
        
        text = "Hello World"
        sha256_hash = hashlib.sha256(text.encode()).hexdigest()
        
        assert len(sha256_hash) == 64
    
    def test_hash_consistency(self):
        """Testa consistência de hash"""
        import hashlib
        
        text = "test"
        hash1 = hashlib.md5(text.encode()).hexdigest()
        hash2 = hashlib.md5(text.encode()).hexdigest()
        
        assert hash1 == hash2  # Mesmo input = mesmo hash


class TestRetryUtils:
    """Testes para utilitários de retry"""
    
    def test_retry_decorator_success(self):
        """Testa decorator de retry com sucesso"""
        from functools import wraps
        import time
        
        def retry(max_attempts=3, delay=0.1):
            def decorator(func):
                @wraps(func)
                def wrapper(*args, **kwargs):
                    attempts = 0
                    while attempts < max_attempts:
                        try:
                            return func(*args, **kwargs)
                        except Exception as e:
                            attempts += 1
                            if attempts >= max_attempts:
                                raise
                            time.sleep(delay)
                    return None
                return wrapper
            return decorator
        
        @retry(max_attempts=3)
        def flaky_function():
            return "success"
        
        result = flaky_function()
        assert result == "success"
    
    def test_exponential_backoff(self):
        """Testa cálculo de exponential backoff"""
        def calculate_backoff(attempt, base_delay=1, max_delay=60):
            delay = min(base_delay * (2 ** attempt), max_delay)
            return delay
        
        assert calculate_backoff(0) == 1
        assert calculate_backoff(1) == 2
        assert calculate_backoff(2) == 4
        assert calculate_backoff(3) == 8
        assert calculate_backoff(10) == 60  # Max delay


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

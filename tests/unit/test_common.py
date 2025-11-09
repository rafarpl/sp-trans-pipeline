"""
Testes Unitários - Módulo Common
"""

import pytest
from datetime import datetime, timedelta


class TestConfig:
    """Testes para configuração."""
    
    def test_get_config_returns_singleton(self):
        """Testa se get_config retorna singleton."""
        from src.common.config import get_config
        
        config1 = get_config()
        config2 = get_config()
        
        assert config1 is config2
    
    def test_config_has_postgres_attributes(self):
        """Testa se Config tem atributos PostgreSQL."""
        from src.common.config import get_config
        
        config = get_config()
        
        assert hasattr(config, "POSTGRES_HOST")
        assert hasattr(config, "POSTGRES_PORT")
        assert hasattr(config, "POSTGRES_DB")
        assert config.POSTGRES_HOST == "localhost"
        assert config.POSTGRES_PORT == 5432


class TestValidators:
    """Testes para validadores."""
    
    def test_validate_coordinates_valid(self):
        """Testa validação de coordenadas válidas."""
        from src.common.validators import validate_coordinates
        
        # Avenida Paulista
        assert validate_coordinates(-23.561684, -46.656139) is True
        
        # Centro de SP
        assert validate_coordinates(-23.550520, -46.633308) is True
    
    def test_validate_coordinates_invalid_latitude(self):
        """Testa validação de latitude inválida."""
        from src.common.validators import validate_coordinates
        
        # Muito ao norte
        assert validate_coordinates(-10.0, -46.633308) is False
        
        # Muito ao sul
        assert validate_coordinates(-25.0, -46.633308) is False
    
    def test_validate_coordinates_invalid_longitude(self):
        """Testa validação de longitude inválida."""
        from src.common.validators import validate_coordinates
        
        # Muito a leste
        assert validate_coordinates(-23.550520, -45.0) is False
        
        # Muito a oeste
        assert validate_coordinates(-23.550520, -48.0) is False
    
    def test_validate_speed_valid(self):
        """Testa validação de velocidade válida."""
        from src.common.validators import validate_speed
        
        assert validate_speed(0.0) is True
        assert validate_speed(25.0) is True
        assert validate_speed(80.0) is True
        assert validate_speed(120.0) is True
    
    def test_validate_speed_invalid(self):
        """Testa validação de velocidade inválida."""
        from src.common.validators import validate_speed
        
        assert validate_speed(-10.0) is False
        assert validate_speed(150.0) is False
    
    def test_validate_speed_none(self):
        """Testa validação de velocidade None."""
        from src.common.validators import validate_speed
        
        assert validate_speed(None) is False
    
    def test_validate_timestamp_valid(self):
        """Testa validação de timestamp válido."""
        from src.common.validators import validate_timestamp
        
        now = datetime.now()
        assert validate_timestamp(now) is True
        
        # 1 hora atrás
        one_hour_ago = now - timedelta(hours=1)
        assert validate_timestamp(one_hour_ago) is True
    
    def test_validate_timestamp_too_old(self):
        """Testa validação de timestamp muito antigo."""
        from src.common.validators import validate_timestamp
        
        old_time = datetime.now() - timedelta(days=2)
        assert validate_timestamp(old_time) is False
    
    def test_validate_vehicle_id_valid(self):
        """Testa validação de vehicle_id válido."""
        from src.common.validators import validate_vehicle_id
        
        assert validate_vehicle_id("12345") is True
        assert validate_vehicle_id("ABC123") is True
    
    def test_validate_vehicle_id_invalid(self):
        """Testa validação de vehicle_id inválido."""
        from src.common.validators import validate_vehicle_id
        
        assert validate_vehicle_id("") is False
        assert validate_vehicle_id(None) is False
        assert validate_vehicle_id("   ") is False


class TestExceptions:
    """Testes para exceções customizadas."""
    
    def test_sptrans_pipeline_exception(self):
        """Testa exceção base."""
        from src.common.exceptions import SPTransPipelineException
        
        with pytest.raises(SPTransPipelineException):
            raise SPTransPipelineException("Test error")
    
    def test_exception_inheritance(self):
        """Testa herança de exceções."""
        from src.common.exceptions import (
            SPTransPipelineException,
            DataValidationException,
            ConfigurationException
        )
        
        assert issubclass(DataValidationException, SPTransPipelineException)
        assert issubclass(ConfigurationException, SPTransPipelineException)

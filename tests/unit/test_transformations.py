"""
Testes Unitários - Módulo Transformations
"""

import pytest


class TestBasicValidation:
    """Testes básicos de validação."""
    
    def test_validators_exist(self):
        """Testa se validators existem."""
        from src.common.validators import validate_coordinates
        
        # Teste simples
        assert validate_coordinates(-23.5, -46.6) is True

# 📡 API Reference - SPTrans Olho Vivo

## Visão Geral

A API Olho Vivo da SPTrans fornece dados em tempo real sobre a posição dos ônibus da cidade de São Paulo.

**Base URL**: `http://api.olhovivo.sptrans.com.br/v2.1`

**Documentação Oficial**: https://www.sptrans.com.br/desenvolvedores/

---

## 🔐 Autenticação

### 1. Obter Token de Autenticação

```http
POST /Login/Autenticar?token={SEU_TOKEN}
```

**Parâmetros:**
- `token`: Token fornecido ao cadastrar-se como desenvolvedor

**Response:**
```
true  (autenticado com sucesso)
false (falha na autenticação)
```

**Exemplo Python:**
```python
import requests

API_URL = "http://api.olhovivo.sptrans.com.br/v2.1"
TOKEN = "seu_token_aqui"

session = requests.Session()
response = session.post(f"{API_URL}/Login/Autenticar?token={TOKEN}")

if response.text == "true":
    print("✅ Autenticado com sucesso!")
else:
    print("❌ Falha na autenticação")
```

---

## 🚍 Endpoints Principais

### 2. Buscar Posição de Todos os Ônibus

```http
GET /Posicao
```

**Headers:**
- `Cookie`: Session cookie obtido após autenticação

**Response:**
```json
{
  "hr": "18:45",
  "l": [
    {
      "c": "8000",
      "cl": 1234,
      "sl": 1,
      "lt0": "Terminal Pinheiros",
      "lt1": "Metrô Vila Madalena",
      "qv": 15,
      "vs": [
        {
          "p": 12345,
          "a": true,
          "ta": "2024-01-20T18:45:30",
          "py": -23.5505,
          "px": -46.6333
        }
      ]
    }
  ]
}
```

**Campos:**
- `hr`: Horário de referência da consulta
- `l`: Lista de linhas (routes)
  - `c`: Código identificador da linha
  - `cl`: Código da linha (legível)
  - `sl`: Sentido da linha (1 ou 2)
  - `lt0`: Letreiro completo (ida)
  - `lt1`: Letreiro completo (volta)
  - `qv`: Quantidade de veículos localizados
  - `vs`: Veículos da linha
    - `p`: Prefixo do veículo (ID único)
    - `a`: Acessibilidade (true/false)
    - `ta`: Data/hora da localização
    - `py`: Latitude
    - `px`: Longitude

---

### 3. Buscar Posição de Uma Linha Específica

```http
GET /Posicao/Linha?codigoLinha={codigo}
```

**Parâmetros:**
- `codigoLinha`: Código da linha (ex: 8000)

**Response:** Mesmo formato do endpoint `/Posicao`, mas apenas com dados da linha solicitada

---

### 4. Listar Todas as Linhas

```http
GET /Linha/Buscar?termosBusca={termo}
```

**Parâmetros:**
- `termosBusca`: Termo para busca (opcional, use "" para todas)

**Response:**
```json
[
  {
    "cl": 8000,
    "lc": false,
    "lt": "Terminal Pinheiros - Metrô Vila Madalena",
    "sl": 1,
    "tl": 10,
    "tp": "Ônibus Municipal",
    "ts": "TERM. PINHEIROS"
  }
]
```

**Campos:**
- `cl`: Código da linha
- `lc`: Linha circular (true/false)
- `lt`: Letreiro completo
- `sl`: Sentido
- `tl`: Tipo de linha
- `tp`: Tipo de transporte
- `ts`: Terminal de origem

---

### 5. Buscar Paradas

```http
GET /Parada/Buscar?termosBusca={termo}
```

**Response:**
```json
[
  {
    "cp": 1234,
    "np": "Av Paulista, 1000",
    "ed": "Av Paulista",
    "py": -23.5505,
    "px": -46.6333
  }
]
```

**Campos:**
- `cp`: Código da parada
- `np`: Nome da parada
- `ed`: Endereço
- `py`: Latitude
- `px`: Longitude

---

### 6. Previsão de Chegada em Parada

```http
GET /Previsao/Parada?codigoParada={codigo}
```

**Response:**
```json
{
  "hr": "18:45",
  "p": {
    "cp": 1234,
    "np": "Av Paulista, 1000",
    "py": -23.5505,
    "px": -46.6333,
    "l": [
      {
        "c": "8000",
        "cl": 8000,
        "sl": 1,
        "lt0": "Terminal Pinheiros",
        "lt1": "Metrô Vila Madalena",
        "qv": 2,
        "vs": [
          {
            "p": 12345,
            "a": true,
            "ta": "2024-01-20T18:50:00",
            "t": "00:05"
          }
        ]
      }
    ]
  }
}
```

---

## 📊 Limites e Restrições

### Rate Limits
- **Requisições por minuto**: Não especificado oficialmente
- **Recomendação**: Intervalo de 2 minutos entre chamadas ao `/Posicao`
- **Timeout de sessão**: ~20 minutos de inatividade

### Boas Práticas
1. **Reautenticar periodicamente**: Manter sessão ativa
2. **Implementar retry logic**: Para falhas temporárias
3. **Cachear dados estáticos**: Linhas, paradas (não mudam frequentemente)
4. **Validar coordenadas**: Verificar se lat/long estão dentro de SP
5. **Tratar dados ausentes**: Nem todos os campos são sempre preenchidos

---

## 🔧 Códigos de Erro Comuns

| Código | Descrição | Solução |
|--------|-----------|---------|
| 401 | Não autenticado | Realizar login novamente |
| 404 | Recurso não encontrado | Verificar código da linha/parada |
| 500 | Erro interno do servidor | Aguardar e tentar novamente |
| 503 | Serviço indisponível | Implementar retry com backoff |

---

## 🧪 Exemplo Completo de Uso

```python
import requests
import time
from datetime import datetime

class SPTransClient:
    def __init__(self, token):
        self.base_url = "http://api.olhovivo.sptrans.com.br/v2.1"
        self.token = token
        self.session = requests.Session()
        self.authenticate()
    
    def authenticate(self):
        """Autentica na API"""
        url = f"{self.base_url}/Login/Autenticar?token={self.token}"
        response = self.session.post(url)
        
        if response.text != "true":
            raise Exception("Falha na autenticação")
        
        print(f"✅ Autenticado em {datetime.now()}")
    
    def get_all_positions(self):
        """Obtém posição de todos os ônibus"""
        url = f"{self.base_url}/Posicao"
        response = self.session.get(url)
        
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 401:
            # Reautenticar e tentar novamente
            self.authenticate()
            return self.get_all_positions()
        else:
            raise Exception(f"Erro: {response.status_code}")
    
    def get_line_positions(self, line_code):
        """Obtém posição de uma linha específica"""
        url = f"{self.base_url}/Posicao/Linha?codigoLinha={line_code}"
        response = self.session.get(url)
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Erro: {response.status_code}")

# Uso
if __name__ == "__main__":
    client = SPTransClient(token="SEU_TOKEN")
    
    # Coletar posições a cada 2 minutos
    while True:
        try:
            positions = client.get_all_positions()
            print(f"📍 Coletados {len(positions.get('l', []))} linhas")
            
            # Processar dados...
            
            time.sleep(120)  # Aguardar 2 minutos
            
        except Exception as e:
            print(f"❌ Erro: {e}")
            time.sleep(60)  # Aguardar 1 minuto em caso de erro
```

---

## 📚 Recursos Adicionais

### Links Úteis
- **Portal Desenvolvedor**: https://www.sptrans.com.br/desenvolvedores/
- **Cadastro API**: https://www.sptrans.com.br/desenvolvedores/cadastro/
- **Suporte**: desenvolvedores@sptrans.com.br

### Dados GTFS
- **Download**: https://www.sptrans.com.br/desenvolvedores/gtfs/
- **Atualização**: Diária (3h da manhã)
- **Formato**: ZIP contendo arquivos TXT

---

## 🎯 Próximos Passos

1. Cadastrar-se como desenvolvedor no portal SPTrans
2. Obter token de API
3. Testar endpoints no notebook `03_api_testing.ipynb`
4. Implementar coleta automatizada com Airflow
5. Armazenar dados no Data Lake (MinIO)

---

**Última atualização**: Janeiro 2025

import requests
from requests.exceptions import RequestException

def get_cities(estado: str) -> list:
    """
    Obtém lista de cidades de um estado via API da Casa dos Dados.

    Args:
        estado: Sigla do estado (ex.: 'SP', 'RJ') ou 'Todos Estados'

    Returns:
        Lista de cidades do estado
    """
    if estado == 'Todos Estados':
        return []  # Não há endpoint para todos os estados, retorna vazio

    try:
        response = requests.get(f"https://api.casadosdados.com.br/v4/public/cnpj/busca/municipio/{estado}", timeout=5)
        response.raise_for_status()  # Levanta exceção para status 4xx/5xx
        cities = [city["name"] for city in response.json()]
        return sorted(cities)  # Ordena para consistência
    except RequestException as e:
        print(f"Erro ao consultar API para UF {estado}: {e}")
        return []
    except ValueError as e:
        print(f"Erro ao processar resposta da API para UF {estado}: {e}")
        return []
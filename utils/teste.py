import requests

teste = requests.get('https://api.casadosdados.com.br/v4/public/cnpj/busca/municipio/SP')

print(teste.json())
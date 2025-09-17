import os
import requests
from dotenv import load_dotenv

# carregar .env (API_KEY e API_URL)
load_dotenv()
API_URL = os.getenv("API_URL")
API_KEY = os.getenv("API_KEY")

# um CNPJ que você sabe que está/esteve no Simples
CNPJ_TESTE = "27865757000102"  

headers = {"Authorization": f"Bearer {API_KEY}"}
params = {"cnpj": CNPJ_TESTE}

resp = requests.get(API_URL, headers=headers, params=params)

print("Status HTTP:", resp.status_code)
try:
    data = resp.json()
    print("Resposta JSON bruta:\n")
    import json
    print(json.dumps(data, indent=2, ensure_ascii=False))
except Exception as e:
    print("Erro ao converter JSON:", e)
    print(resp.text)

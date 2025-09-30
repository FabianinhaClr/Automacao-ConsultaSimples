import requests

API_KEY = "cole_aqui_sua_chave"
url = "https://api.cnpja.com.br/status"

headers = {"Authorization": f"Bearer {API_KEY}"}
r = requests.get(url, headers=headers)

print("Status:", r.status_code)
print("Resposta:", r.text)

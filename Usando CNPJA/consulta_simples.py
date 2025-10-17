import requests
from datetime import datetime

API_KEY = "a7113114-1944-4b53-a30a-4d1dc8456122-ac9675f9-87f5-49d4-945a-c908df24e1fb"  # Substitua pela sua chave

def consultar_simples_cnpja(cnpj):
    cnpj = ''.join(filter(str.isdigit, cnpj))
    if len(cnpj) != 14:
        print("CNPJ inválido!")
        return

    url = f"https://open.cnpja.com/office/{cnpj}"
    headers = {"Authorization": f"Bearer {API_KEY}"}

    try:
        r = requests.get(url, headers=headers)
        dados = r.json()

        print(f"CNPJ: {cnpj}")

        # O histórico pode estar em 'simples' ou em 'optante_simples'
        historico = dados.get("simples") or dados.get("optante_simples") or []

        if not historico:
            print("Nenhum histórico de Simples Nacional encontrado.")
            return

        anos = list(range(datetime.now().year - 5, datetime.now().year))
        for ano in anos:
            regime = "Não Simples"
            for periodo in historico:
                dt_ini = periodo.get("data_inicio")
                dt_fim = periodo.get("data_fim", datetime.now().strftime("%Y-%m-%d"))
                detalhe = periodo.get("detalhamento", "")

                if dt_ini:
                    dt_ini = datetime.strptime(dt_ini, "%Y-%m-%d")
                    dt_fim = datetime.strptime(dt_fim, "%Y-%m-%d")
                    if dt_ini.year <= ano <= dt_fim.year:
                        regime = f"Simples Nacional ({detalhe})" if detalhe else "Simples Nacional"
                        break
            print(f"Ano {ano}: {regime}")

    except Exception as e:
        print(f"Erro ao consultar CNPJ: {e}")

# Exemplo de uso
cnpj = input("Digite o CNPJ (apenas números): ")
consultar_simples_cnpja(cnpj)

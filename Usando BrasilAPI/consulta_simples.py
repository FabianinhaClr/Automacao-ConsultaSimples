import requests
import pandas as pd
from datetime import datetime
import time

INPUT_FILE = "cnpjs.txt"
OUTPUT_FILE = "resultado_brasilapi.xlsx"

def read_cnpjs(path):
    with open(path, "r") as f:
        return [line.strip() for line in f if line.strip()]

def parse_date(date_str):
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except:
        return None

def extract_simples_years(j):
    simples_years = set()
    reasons = []

    # 1) Dados de regime_tributario por ano
    for entry in j.get("regime_tributario", []) or []:
        ano = entry.get("ano")
        forma = entry.get("forma_de_tributacao") or ""
        if forma and "SIMPLES" in forma.upper():
            try:
                ano_int = int(ano)
                if 2020 <= ano_int <= 2025:
                    simples_years.add(ano_int)
                    reasons.append(f"regime_tributario:{ano_int}:{forma}")
            except Exception:
                reasons.append(f"regime_tributario:ano_invalido:{forma}")

    # 2) Período de opção e exclusão
    data_inicio = parse_date(j.get("data_opcao_pelo_simples"))
    data_fim = parse_date(j.get("data_exclusao_do_simples"))

    if data_inicio:
        start_year = max(data_inicio.year, 2020)
        end_year = min((data_fim.year if data_fim else 2025), 2025)

        for y in range(start_year, end_year + 1):
            simples_years.add(y)

        reasons.append(f"periodo:{data_inicio.date()} - {data_fim.date() if data_fim else 'atual'}")

    return simples_years, reasons

def consultar_cnpj(cnpj):
    url = f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            return resp.json(), None
        elif resp.status_code == 429:
            return None, "Erro 429 (muitas requisições)"
        else:
            return None, f"Erro {resp.status_code}"
    except Exception as e:
        return None, f"Exception:{e}"

def processar():
    cnpjs = read_cnpjs(INPUT_FILE)
    resultados = []

    for cnpj in cnpjs:
        print(f"🔎 Consultando {cnpj}...")
        j, erro = consultar_cnpj(cnpj)
        if erro:
            for ano in range(2020, 2025 + 1):
                resultados.append({
                    "CNPJ+ANO": f"{cnpj}{ano}",
                    "CNPJ": cnpj,
                    "Ano": ano,
                    "Regime": "ERRO",
                    "Motivo": erro
                })
            continue

        simples_years, reasons = extract_simples_years(j)

        for ano in range(2020, 2025 + 1):
            if ano in simples_years:
                regime = "Simples Nacional"
                motivo = ";".join(reasons)
            else:
                regime = "Outro Regime"
                motivo = "sem_evidencia_simples"
            resultados.append({
                "CNPJ+ANO": f"{cnpj}{ano}",
                "CNPJ": cnpj,
                "Ano": ano,
                "Regime": regime,
                "Motivo": motivo
            })

        time.sleep(0.5)  # Respeitar limite da API

    df = pd.DataFrame(resultados)
    df.to_excel(OUTPUT_FILE, index=False)
    print(f"✅ Resultados salvos em {OUTPUT_FILE}")

if __name__ == "__main__":
    processar()

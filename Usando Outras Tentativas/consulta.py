import os
import re
import time
from datetime import datetime, date
from dotenv import load_dotenv
import requests
import pandas as pd
from tqdm import tqdm
from pathlib import Path
from dateutil import parser as dparser

# ==============================
# CONFIG
# ==============================
load_dotenv()
API_KEY = os.getenv("API_KEY")  # token da ReceitaWS
INPUT_FILE = "cnpjs.txt"
OUTPUT_FILE = "TESTE.xlsx"
YEARS = list(range(2020, 2026))
SLEEP = 21.0   # respeitar limite de 3 req/minuto (1 req a cada ~20s)
DEBUG = False

# ==============================
# FUNÇÕES
# ==============================
def clean_cnpj(s):
    return re.sub(r'\D', '', str(s)).zfill(14)

def read_cnpjs(path):
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"{path} não encontrado. Crie com 1 CNPJ por linha.")
    with open(p, "r", encoding="utf-8") as f:
        return [clean_cnpj(line.strip()) for line in f if line.strip()]

def query_receitaws(cnpj):
    """
    Consulta API ReceitaWS (https://developers.receitaws.com.br)
    """
    try:
        url = f"https://www.receitaws.com.br/v1/cnpj/{cnpj}"
        if API_KEY:
            url += f"?token={API_KEY}"
        r = requests.get(url, timeout=60)
        try:
            j = r.json()
        except Exception:
            j = None
        return r.status_code, j
    except Exception as e:
        if DEBUG:
            print(f"[DEBUG] Erro na requisição para {cnpj}: {e}")
        return None, None

def parse_date_any(s):
    if not s:
        return None
    s = str(s).strip()
    formatos = ["%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d"]
    for fmt in formatos:
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    try:
        return dparser.parse(s, dayfirst=True).date()
    except Exception:
        return None

def extract_periods_from_response(resp_json):
    """
    ReceitaWS só retorna informações atuais sobre Simples Nacional:
      - "opcao_pelo_simples" (True/False)
      - "data_opcao_pelo_simples"
      - "data_exclusao_do_simples"
    """
    periods = []
    if not resp_json or not isinstance(resp_json, dict):
        return periods

    if resp_json.get("opcao_pelo_simples") is True:
        si = parse_date_any(resp_json.get("data_opcao_pelo_simples"))
        ei = parse_date_any(resp_json.get("data_exclusao_do_simples"))
        if si:
            periods.append({
                "start": si,
                "end": ei,
                "detalhe": "ReceitaWS - opção pelo Simples"
            })

    return periods

def periods_to_string(periods):
    parts = []
    for p in periods:
        s = p.get("start").isoformat() if p.get("start") else ""
        e = p.get("end").isoformat() if p.get("end") else "até hoje"
        det = p.get("detalhe") or ""
        parts.append(f"{s} - {e} [{det}]")
    return "; ".join(parts)

def covers_year_with_rules(periods, year, consulta_date=None):
    if consulta_date is None:
        consulta_date = date.today()
    year_start = date(year, 1, 1)
    year_end = date(year, 12, 31)
    motivos = []

    for p in periods:
        si = p.get("start")
        ei = p.get("end")
        detalhe = (p.get("detalhe") or "").strip()
        if not si:
            continue
        ei_eff = ei if ei else consulta_date

        if year == consulta_date.year:
            if si <= year_start and ei_eff >= consulta_date:
                motivos.append(f"cobre {year} até {consulta_date.isoformat()} [{detalhe}]")
                return True, "; ".join(motivos)
        if si <= year_start and ei_eff >= year_end:
            motivos.append(f"cobre {year} inteiro [{detalhe}]")
            return True, "; ".join(motivos)

    return False, "nenhum_periodo_encontrado"

# ==============================
# MAIN
# ==============================
def main():
    cnpjs = read_cnpjs(INPUT_FILE)
    rows = []

    for cnpj in tqdm(cnpjs, desc="Consultando CNPJs"):
        status, resp_json = query_receitaws(cnpj)

        if status is None or resp_json is None:
            for year in YEARS:
                rows.append({
                    "CNPJ+ANO": f"{cnpj}/{year}",
                    "CNPJ": cnpj,
                    "Ano": year,
                    "Regime": "ERRO_CONSULTA",
                    "Motivo": "erro_requisicao_ou_resp",
                    "Períodos_detectados": ""
                })
            continue

        if resp_json.get("status") == "ERROR":
            msg = resp_json.get("message", "sem mensagem")
            for year in YEARS:
                rows.append({
                    "CNPJ+ANO": f"{cnpj}/{year}",
                    "CNPJ": cnpj,
                    "Ano": year,
                    "Regime": "API_ERROR",
                    "Motivo": msg,
                    "Períodos_detectados": ""
                })
            continue

        situacao_atual = resp_json.get("situacao")
        periods = extract_periods_from_response(resp_json)
        periods_str = periods_to_string(periods)

        for year in YEARS:
            is_opt, motivo = covers_year_with_rules(periods, year)
            regime = "Simples Nacional" if is_opt else "Outro Regime"
            rows.append({
                "CNPJ+ANO": f"{cnpj}/{year}",
                "CNPJ": cnpj,
                "Ano": year,
                "Regime": regime,
                "Motivo": motivo,
                "Períodos_detectados": periods_str,
                "Situacao_Atual": situacao_atual or ""
            })

        # respeitar limite da ReceitaWS Free
        time.sleep(SLEEP)

    df = pd.DataFrame(rows, columns=["CNPJ+ANO", "CNPJ", "Ano", "Regime", "Motivo", "Períodos_detectados", "Situacao_Atual"])
    df.to_excel(OUTPUT_FILE, index=False)
    print(f"\n✅ Consulta finalizada. Planilha salva em {OUTPUT_FILE}")


if __name__ == "__main__":
    main()

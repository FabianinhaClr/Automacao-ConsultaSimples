# consulta_simples_v2.py
import os
import re
import json
import time
from datetime import datetime, date
from dotenv import load_dotenv
import requests
import pandas as pd
from tqdm import tqdm
from pathlib import Path
from dateutil import parser as dparser

load_dotenv()
API_URL = os.getenv("API_URL")  # https://api.infosimples.com/api/v2/consultas/receita-federal/simples
API_KEY = os.getenv("API_KEY")

INPUT_FILE = "cnpjs.txt"   # coloque só um CNPJ para testar (ex: cnpjs_single.txt)
OUTPUT_FILE = "resultado_simples_2020_2025_v2.xlsx"
DEBUG_DIR = "debug_responses_v2"
YEARS = list(range(2020, 2026))
SLEEP = 0.5  # pausa entre chamadas (ajuste se necessário)

os.makedirs(DEBUG_DIR, exist_ok=True)

def clean_cnpj(s):
    return re.sub(r'\D', '', str(s)).zfill(14)

def read_cnpjs(path):
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"{path} não encontrado. Crie com 1 CNPJ por linha.")
    with open(p, "r", encoding="utf-8") as f:
        return [clean_cnpj(line.strip()) for line in f if line.strip()]

def save_debug(cnpj, response_text, response_json=None):
    out = {
        "_fetched_at": datetime.utcnow().isoformat() + "Z",
        "response_text_snippet": (response_text[:200] + "...") if response_text else None,
        "json": response_json
    }
    with open(os.path.join(DEBUG_DIR, f"{cnpj}.json"), "w", encoding="utf-8") as fh:
        json.dump(out, fh, ensure_ascii=False, indent=2)

def query_infosimples(cnpj):
    args = {
        "cnpj": cnpj,
        "token": API_KEY,
        "timeout": 300
    }
    try:
        r = requests.post(API_URL, data=args, timeout=60)
    except Exception as e:
        print(f"[ERRO REQ] {cnpj}: {e}")
        return None, None, getattr(e, "args", str(e))
    txt = r.text
    try:
        j = r.json()
    except Exception:
        j = None
    save_debug(cnpj, txt, j)
    return r.status_code, j, txt

def parse_date_any(s):
    if not s:
        return None
    if isinstance(s, (int, float)):
        s = str(int(s))
    s = str(s).strip()
    # tenta dd/mm/YYYY
    try:
        return datetime.strptime(s, "%d/%m/%Y").date()
    except Exception:
        pass
    # tenta ISO ou heurístico via dateutil
    try:
        return dparser.parse(s, dayfirst=True).date()
    except Exception:
        return None

def extract_periods_from_response(resp_json):
    """
    Retorna lista de períodos como tuples (start_date, end_date_or_None)
    Procura em: resp['data'][0]['simples_nacional_periodos_anteriores'] ou variantes.
    """
    periods = []
    if not resp_json or not isinstance(resp_json, dict):
        return periods
    # resp['data'] costuma ser lista
    data = None
    if 'data' in resp_json:
        if isinstance(resp_json['data'], list) and resp_json['data']:
            data = resp_json['data'][0]
        elif isinstance(resp_json['data'], dict):
            data = resp_json['data']
    # procurar chaves comuns
    candidate_keys = [
        "simples_nacional_periodos_anteriores",
        "simples_nacional_periodos",
        "periodos_simples",
        "simples_periodos",
        "simples_nacional"
    ]
    if data and isinstance(data, dict):
        for k in candidate_keys:
            if k in data and isinstance(data[k], list):
                for item in data[k]:
                    if not isinstance(item, dict):
                        continue
                    s = item.get("inicio_data") or item.get("inicio") or item.get("data_inicio") or item.get("normalizado_inicio_data")
                    e = item.get("fim_data") or item.get("fim") or item.get("data_fim") or item.get("normalizado_fim_data")
                    si = parse_date_any(s)
                    ei = parse_date_any(e)
                    periods.append((si, ei))
                return periods
    # heurística recursiva: procurar listas de dicts em qualquer lugar do json
    def find_lists(obj):
        results = []
        if isinstance(obj, list):
            if obj and isinstance(obj[0], dict):
                # pode ser lista de períodos
                results.append(obj)
            for v in obj:
                results += find_lists(v)
        elif isinstance(obj, dict):
            for v in obj.values():
                results += find_lists(v)
        return results
    lists = find_lists(resp_json)
    for lst in lists:
        for item in lst:
            if not isinstance(item, dict):
                continue
            s = item.get("inicio_data") or item.get("inicio") or item.get("data_inicio") or item.get("normalizado_inicio_data") or item.get("data")
            e = item.get("fim_data") or item.get("fim") or item.get("data_fim") or item.get("normalizado_fim_data")
            si = parse_date_any(s)
            ei = parse_date_any(e)
            if si:
                periods.append((si, ei))
    return periods

def covers_year(periods, situacao_atual, year):

    start = date(year, 1, 1)
    end = date(year, 12, 31)
    # checar períodos explícitos
    for (si, ei) in periods:
        if not si:
            continue
        ei_eff = ei or date(9999, 12, 31)
        if si <= end and ei_eff >= start:
            return True, f"coberto_por_periodo {si.isoformat()} - {(ei_eff.isoformat() if ei else 'ongoing')}"
    # fallback: marcar apenas ano atual se situacao_atual indica Optante
    if situacao_atual and isinstance(situacao_atual, str) and "OPTANTE" in situacao_atual.upper():
        current_year = date.today().year
        if year == current_year:
            return True, "fallback_situacao_atual_ano_corrente"
    return False, "outro_regime"

def main():
    cnpjs = read_cnpjs(INPUT_FILE)
    rows = []
    print(f"Consultando {len(cnpjs)} CNPJs (anos {YEARS[0]}-{YEARS[-1]})...")

    for cnpj in tqdm(cnpjs):
        status, resp_json, raw_text = query_infosimples(cnpj)
        if status is None:
            print(f"[ERRO REQ] {cnpj}: sem resposta.")
            # salva linha com erro para checar depois
            for year in YEARS:
                rows.append({"CNPJ+ANO": f"{cnpj}/{year}", "CNPJ": cnpj, "Ano": year, "Regime": "ERRO_REQUISICAO"})
            continue

        if resp_json is None:
            # resposta não-JSON (HTML / 404 / erro). já salva debug no arquivo.
            print(f"[WARN] {cnpj}: resposta não-JSON (status {status}). Verifique debug_responses_v2/{cnpj}.json")
            for year in YEARS:
                rows.append({"CNPJ+ANO": f"{cnpj}/{year}", "CNPJ": cnpj, "Ano": year, "Regime": "ERRO_RESP_NAO_JSON"})
            time.sleep(SLEEP)
            continue

        # checa código API (campo 'code' na resposta)
        code = resp_json.get("code")
        if code != 200:
            msg = resp_json.get("code_message", "sem mensagem")
            print(f"[API] {cnpj}: code={code} msg={msg}")
            for year in YEARS:
                rows.append({"CNPJ+ANO": f"{cnpj}/{year}", "CNPJ": cnpj, "Ano": year, "Regime": f"API_CODE_{code}"})
            time.sleep(SLEEP)
            continue

        # extrair dados relevantes
        data_item = None
        if isinstance(resp_json.get("data"), list) and resp_json["data"]:
            data_item = resp_json["data"][0]
        elif isinstance(resp_json.get("data"), dict):
            data_item = resp_json["data"]

        situacao_atual = data_item.get("simples_nacional_situacao") if data_item else None
        periods = extract_periods_from_response(resp_json)

        # imprimir resumo curto no terminal para debug
        print(f"\n>>>> {cnpj} | situacao='{situacao_atual}' | periodos_encontrados={len(periods)}")
        for p in periods:
            si = p[0].isoformat() if p[0] else None
            ei = p[1].isoformat() if p[1] else None
            print("   periodo:", si, "-", (ei or "ongoing"))

        # gerar linhas por ano com motivo
        for year in YEARS:
            is_opt, motivo = covers_year(periods, situacao_atual, year)
            regime = "Simples Nacional" if is_opt else "Outro Regime"
            rows.append({
                "CNPJ+ANO": f"{cnpj}/{year}",
                "CNPJ": cnpj,
                "Ano": year,
                "Regime": regime,
                "Motivo": motivo
            })

        time.sleep(SLEEP)

    df = pd.DataFrame(rows)
    # salvar (caso Excel esteja aberto, vai falhar — feche o arquivo antes)
    df.to_excel(OUTPUT_FILE, index=False)
    print(f"\n✅ Planilha gerada: {OUTPUT_FILE}")
    print(f"Arquivos de debug: {DEBUG_DIR}/<CNPJ>.json")
    print("Se algo estiver errado, cole aqui o conteúdo de um debug_responses_v2/<CNPJ>.json que eu ajusto o parser.")
    
if __name__ == "__main__":
    main()

# consulta_simples.py (versão debug/robusta)
import os
import re
import json
import time
import argparse
from datetime import date, datetime
from dateutil import parser as dparser
from dotenv import load_dotenv
import requests
import pandas as pd
from tqdm import tqdm

load_dotenv()

API_URL = os.getenv("API_URL")
API_KEY = os.getenv("API_KEY")
INPUT_FILE = "cnpjs.txt"
OUTPUT_FILE = "resultado_simples_2020_2025_debug.xlsx"
DEBUG_DIR = "debug_responses"
YEARS = list(range(2020, 2026))

os.makedirs(DEBUG_DIR, exist_ok=True)

def clean_cnpj(s):
    s = str(s)
    nums = re.sub(r'\D', '', s)
    return nums.zfill(14)

def read_cnpjs(path):
    path = str(path)
    if path.lower().endswith(('.xls', '.xlsx')):
        df = pd.read_excel(path, dtype=str)
        col = next((c for c in df.columns if 'cnpj' in c.lower()), df.columns[0])
        return [clean_cnpj(x) for x in df[col].dropna().unique().tolist()]
    else:
        with open(path, 'r', encoding='utf-8') as f:
            return [clean_cnpj(line.strip()) for line in f if line.strip()]

def try_parse_date(s):
    if not s:
        return None
    if isinstance(s, (int, float)):
        # YYYYMMDD?
        s = str(int(s))
    try:
        return dparser.parse(str(s), dayfirst=False).date()
    except Exception:
        m = re.match(r'(\d{4})-?(\d{2})-?(\d{2})', str(s))
        if m:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    return None

def save_debug_response(cnpj, resp_obj, resp_text=None):
    path = os.path.join(DEBUG_DIR, f"{cnpj}.json")
    out = {
        "_fetched_at": datetime.utcnow().isoformat() + "Z",
        "raw_text": None,
        "json": None
    }
    if resp_obj is not None:
        out["json"] = resp_obj
    if resp_text is not None:
        out["raw_text"] = resp_text
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

def query_provider(cnpj):
    headers = {}
    if API_KEY:
        headers["Authorization"] = f"Bearer {API_KEY}"
        headers["x-api-key"] = API_KEY
    params = {"cnpj": cnpj}
    try:
        if not API_URL:
            raise RuntimeError("API_URL não configurada no .env")
        r = requests.get(API_URL, params=params, headers=headers, timeout=30)
    except Exception as e:
        return {"__error": f"request-failed: {e}"}
    # salva resposta bruta
    text = r.text
    try:
        j = r.json()
        save_debug_response(cnpj, j, None)
        return j
    except Exception:
        save_debug_response(cnpj, None, text)
        return {"__text": text, "__status_code": r.status_code}

# procura listas/dicts que pareçam períodos ou mapa ano->status
def find_candidates(obj):
    candidates = []
    if isinstance(obj, dict):
        # mapa ano->valor?
        if any(k.isdigit() and 1900 < int(k) < 2100 for k in obj.keys()):
            candidates.append(("year_map", obj))
        # keys that explicitly mention simples
        for k, v in obj.items():
            kl = k.lower()
            if 'simples' in kl or 'optante' in kl or 'period' in kl or 'opcao' in kl:
                candidates.append(("explicit", {k: v}))
        # recursão
        for v in obj.values():
            candidates.extend(find_candidates(v))
    elif isinstance(obj, list):
        if obj and isinstance(obj[0], dict):
            # lista de dicts -> possível lista de períodos ou ano/status
            keys = set().union(*(d.keys() for d in obj if isinstance(d, dict)))
            klow = {k.lower() for k in keys}
            if any(k in klow for k in ('inicio','inicio_data','data_inicio','start','data','dataInicio','dataInicial','dt_inicio')):
                candidates.append(("period_list", obj))
            elif any(k in klow for k in ('ano','year','situacao','status','regime')):
                candidates.append(("yearstatus_list", obj))
        # recursão
        for v in obj:
            candidates.extend(find_candidates(v))
    return candidates

def is_true_like(v):
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in ("sim", "s", "true", "1", "optante", "optou", "ativo", "ativo(a)", "adesao", "adotou")

def extract_periods_robust(resp):
    """
    Retorna lista de dicts {'inicio_data': ..., 'fim_data': ...}
    """
    if not resp:
        return []
    periods = []
    # busca candidatos recursivamente
    cands = find_candidates(resp)
    for typ, val in cands:
        if typ == "period_list":
            for item in val:
                if not isinstance(item, dict):
                    continue
                # tenta extrair campos de data em várias chaves
                start = None
                end = None
                for k in item.keys():
                    kl = k.lower()
                    if any(x in kl for x in ('inicio','start','data_inicio','data','dataInicio','dataInicial','dt_inicio')):
                        start = item[k] if start is None else start
                    if any(x in kl for x in ('fim','end','data_fim','dt_fim')):
                        end = item[k] if end is None else end
                if start:
                    periods.append({"inicio_data": start, "fim_data": end})
        elif typ == "yearstatus_list":
            for item in val:
                if not isinstance(item, dict):
                    continue
                y = None
                s = None
                for k in item.keys():
                    kl = k.lower()
                    if kl in ('ano','year'):
                        y = item[k]
                    if kl in ('situacao','status','regime','optante'):
                        s = item[k]
                if y and is_true_like(s):
                    periods.append({"inicio_data": f"{int(y)}-01-01", "fim_data": f"{int(y)}-12-31"})
        elif typ == "year_map":
            for k1, v1 in val.items():
                if k1.isdigit():
                    if int(k1) in YEARS and is_true_like(v1):
                        periods.append({"inicio_data": f"{k1}-01-01", "fim_data": f"{k1}-12-31"})
        elif typ == "explicit":
            # pode ser campo único com 'simples' => se for booleano/str true, marca ano atual
            for k2, v2 in val.items():
                if isinstance(v2, (str, bool)) and is_true_like(v2):
                    # marca ano atual como optante (fallback)
                    today = date.today().year
                    periods.append({"inicio_data": f"{today}-01-01", "fim_data": f"{today}-12-31"})
    # fallback: se há campo top-level indicando 'optante' ou 'simples'
    if not periods and isinstance(resp, dict):
        for key in ('optante','simples','simples_nacional','situacao_simples','situacao'):
            if key in resp and is_true_like(resp[key]):
                today = date.today().year
                periods.append({"inicio_data": f"{today}-01-01", "fim_data": f"{today}-12-31"})
                break
        if 'data' in resp and isinstance(resp['data'], dict):
            for key in ('optante','simples','situacao'):
                if key in resp['data'] and is_true_like(resp['data'][key]):
                    today = date.today().year
                    periods.append({"inicio_data": f"{today}-01-01", "fim_data": f"{today}-12-31"})
                    break
    return periods

def periods_cover_year(periods, year):
    if not periods:
        return False
    ystart = date(year, 1, 1)
    yend = date(year, 12, 31)
    for p in periods:
        a = try_parse_date(p.get('inicio_data') or p.get('inicio') or p.get('start'))
        b = try_parse_date(p.get('fim_data') or p.get('fim') or p.get('end'))
        if a is None:
            continue
        if b is None:
            b = date(9999, 12, 31)
        if a <= yend and b >= ystart:
            return True
    return False

def main(args):
    cnpjs = read_cnpjs(INPUT_FILE)
    rows = []
    print(f"Total CNPJs: {len(cnpjs)}")
    for cnpj in tqdm(cnpjs, desc="Consultando"):
        resp = query_provider(cnpj)
        # se resposta tem chave de erro, salva e continua
        if isinstance(resp, dict) and ('__error' in resp or '__status_code' in resp):
            print(f"[WARN] {cnpj}: resposta não-JSON ou erro HTTP: {resp.get('__error') or resp.get('__status_code')}")
            periods = []
        else:
            periods = extract_periods_robust(resp)
        # log resumo
        if args.debug:
            dbgfile = os.path.join(DEBUG_DIR, f"{cnpj}.json")
            print(f" {cnpj}: períodos encontrados = {len(periods)}; debug -> {dbgfile}")
        # gera linhas por ano
        for year in YEARS:
            regime = "Simples Nacional" if periods_cover_year(periods, year) else "Outro Regime"
            rows.append({
                "CNPJ+ANO": f"{cnpj}/{year}",
                "CNPJ": cnpj,
                "Ano": year,
                "Regime": regime
            })
        time.sleep(0.12)
    df = pd.DataFrame(rows, columns=["CNPJ+ANO", "CNPJ", "Ano", "Regime"])
    df.to_excel(OUTPUT_FILE, index=False)
    print("✅ Planilha gerada:", OUTPUT_FILE)
    if args.debug:
        print("Arquivos de debug salvos na pasta:", DEBUG_DIR)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true", help="salva respostas brutas em debug_responses/")
    args = parser.parse_args()
    main(args)

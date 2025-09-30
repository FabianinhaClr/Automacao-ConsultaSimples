import os  # mexe com arquivos, pastas e variáveis do sistema
import re  # expressões regulares, aqui usado pra limpar CNPJs (tirar pontos, traços, etc)
import json  # pra salvar e abrir dados em formato JSON (debug, resposta da API)
import time  # controlar pausas entre as consultas (pra não estourar a API)
from datetime import datetime, date  # lidar com datas (comparar períodos, formatar, etc)
from dotenv import load_dotenv  # carrega as configs do arquivo .env (URL e chave da API)
import requests  # biblioteca que faz as requisições HTTP
import pandas as pd  # organiza resultados em tabelas e joga tudo num Excel no final
from tqdm import tqdm  # gera a barrinha de progresso bonitinha
from pathlib import Path  # forma mais moderna de lidar com arquivos e diretórios
from dateutil import parser as dparser  # parseia datas esquisitas em diferentes formatos

# carrega variáveis do arquivo .env (tipo usuário e senha, mas aqui é a API)
load_dotenv()
API_URL = os.getenv("API_URL")
API_KEY = os.getenv("API_KEY")

# arquivos e configs principais
INPUT_FILE = "cnpjs.txt"  # lista de CNPJs a consultar
OUTPUT_FILE = "resultadoconsulta.xlsx"  # planilha final
DEBUG_DIR = "debug_responses_v3"  # pasta onde guardo as respostas cruas da API
YEARS = list(range(2020, 2026))  # anos que vou verificar se estava no Simples
SLEEP = 0.5  # meio segundo entre as consultas, pra não dar block

# cria a pasta de debug se não existir
os.makedirs(DEBUG_DIR, exist_ok=True)


# --- FUNÇÕES ---

def clean_cnpj(s):
    # pega um CNPJ, tira tudo que não é número e preenche até 14 dígitos
    return re.sub(r'\D', '', str(s)).zfill(14)


def read_cnpjs(path):
    # abre o arquivo de entrada com os CNPJs
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"{path} não encontrado. Crie com 1 CNPJ por linha.")
    with open(p, "r", encoding="utf-8") as f:
        # limpa cada linha e garante que o CNPJ esteja formatado certinho
        return [clean_cnpj(line.strip()) for line in f if line.strip()]


def save_debug(cnpj, response_text, response_json=None):
    # salva a resposta da API (texto + json) num arquivo separado pra debug
    out = {
        "_fetched_at": datetime.utcnow().isoformat() + "Z",  # data/hora da consulta
        "response_text_snippet": (response_text[:400] + "...") if response_text else None,  # só os primeiros caracteres da resposta
        "json": response_json  # json inteiro (se tiver)
    }
    with open(os.path.join(DEBUG_DIR, f"{cnpj}.json"), "w", encoding="utf-8") as fh:
        json.dump(out, fh, ensure_ascii=False, indent=2)


def query_infosimples(cnpj):
    # manda requisição pra API com o CNPJ e a chave
    args = {"cnpj": cnpj, "token": API_KEY, "timeout": 300}
    try:
        r = requests.post(API_URL, data=args, timeout=60)
    except Exception as e:
        # se deu erro de rede ou API caiu
        print(f"[ERRO REQ] {cnpj}: {e}")
        return None, None, getattr(e, "args", str(e))
    txt = r.text
    try:
        j = r.json()  # tenta transformar a resposta em JSON
    except Exception:
        j = None  # se não rolar, marca como None
    save_debug(cnpj, txt, j)  # salva no debug pra investigar depois
    return r.status_code, j, txt


def parse_date_any(s):
    # tenta converter qualquer coisa em data
    if not s:
        return None
    if isinstance(s, (int, float)):  # tipo 20240101
        s = str(int(s))
    s = str(s).strip()
    try:
        # formato padrão BR: dd/mm/yyyy
        return datetime.strptime(s, "%d/%m/%Y").date()
    except Exception:
        try:
            # se falhar, usa o parser "esperto" que tenta adivinhar
            return dparser.parse(s, dayfirst=True).date()
        except Exception:
            return None


def extract_periods_from_response(resp_json):
    # procura dentro da resposta da API os períodos em que a empresa esteve no Simples
    periods = []
    if not resp_json or not isinstance(resp_json, dict):
        return periods

    # pode vir como lista ou dicionário dentro de "data"
    data = None
    if 'data' in resp_json:
        if isinstance(resp_json['data'], list) and resp_json['data']:
            data = resp_json['data'][0]
        elif isinstance(resp_json['data'], dict):
            data = resp_json['data']

    # várias APIs usam nomes diferentes pras mesmas coisas... então testamos todas
    candidate_keys = [
        "simples_nacional_periodos_anteriores",
        "simples_nacional_periodos",
        "periodos_simples",
        "simples_periodos",
        "simples_nacional"
    ]

    # percorre os possíveis campos
    if data and isinstance(data, dict):
        for k in candidate_keys:
            if k in data and isinstance(data[k], list):
                for item in data[k]:
                    if not isinstance(item, dict):
                        continue
                    # pega datas de início e fim
                    s = item.get("inicio_data") or item.get("inicio") or item.get("data_inicio") or item.get("normalizado_inicio_data")
                    e = item.get("fim_data") or item.get("fim") or item.get("data_fim") or item.get("normalizado_fim_data")
                    si = parse_date_any(s)
                    ei = parse_date_any(e)
                    if si:
                        periods.append((si, ei))
                return periods

    # se não achou nada, vasculha o JSON inteiro (função recursiva)
    def find_lists(obj):
        results = []
        if isinstance(obj, list):
            if obj and isinstance(obj[0], dict):
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


def covers_year_strict(periods, year):
    # verifica se um certo ano está dentro de algum período do Simples
    start = date(year, 1, 1)
    end = date(year, 12, 31)
    for (si, ei) in periods:
        if not si:
            continue
        ei_eff = ei or date(9999, 12, 31)  # se não tem fim, considera "aberto"
        if si <= end and ei_eff >= start:
            return True, f"coberto_por_periodo {si.isoformat()} - {(ei_eff.isoformat() if ei else 'ongoing')}"
    return False, "sem_periodo_explicito"


def main():
    # carrega lista de CNPJs
    cnpjs = read_cnpjs(INPUT_FILE)
    rows = []
    print(f"Consultando {len(cnpjs)} CNPJs (anos {YEARS[0]}-{YEARS[-1]})...")

    for cnpj in tqdm(cnpjs):
        status, resp_json, raw_text = query_infosimples(cnpj)

        # se nem conseguiu consultar
        if status is None:
            print(f"[ERRO REQ] {cnpj}: sem resposta.")
            for year in YEARS:
                rows.append({"CNPJ+ANO": f"{cnpj}/{year}", "CNPJ": cnpj, "Ano": year, "Regime": "ERRO_REQUISICAO", "Motivo": "erro_requisicao"})
            continue

        # se veio algo que não é JSON
        if resp_json is None:
            print(f"[WARN] {cnpj}: resposta não-JSON (status {status}). Verifique {DEBUG_DIR}/{cnpj}.json")
            for year in YEARS:
                rows.append({"CNPJ+ANO": f"{cnpj}/{year}", "CNPJ": cnpj, "Ano": year, "Regime": "ERRO_RESP_NAO_JSON", "Motivo": "resp_nao_json"})
            time.sleep(SLEEP)
            continue

        # se a API respondeu com erro (tipo code 400, 401, etc)
        code = resp_json.get("code")
        if code != 200:
            msg = resp_json.get("code_message", "sem mensagem")
            for year in YEARS:
                rows.append({
                    "CNPJ+ANO": f"{cnpj}/{year}",
                    "CNPJ": cnpj,
                    "Ano": year,
                    "Regime": f"API_CODE_{code}",
                    "Motivo": msg
                })
            continue

        # pega o bloco "data" (pode ser lista ou dict)
        data_item = None
        if isinstance(resp_json.get("data"), list) and resp_json["data"]:
            data_item = resp_json["data"][0]
        elif isinstance(resp_json.get("data"), dict):
            data_item = resp_json["data"]

        # situação atual no Simples (ativo, excluído, etc)
        situacao_atual = data_item.get("simples_nacional_situacao") if data_item else None
        # períodos encontrados
        periods = extract_periods_from_response(resp_json)

        # print de debug no console
        print(f"\n>>>> {cnpj} | situacao='{situacao_atual}' | periodos_encontrados={len(periods)}")
        for p in periods:
            si = p[0].isoformat() if p[0] else None
            ei = p[1].isoformat() if p[1] else None
            print("   periodo:", si, "-", (ei or "ongoing"))

        # gera resultado pra cada ano
        for year in YEARS:
            is_opt, motivo = covers_year_strict(periods, year)
            regime = "Simples Nacional" if is_opt else "Outro Regime"
            rows.append({
                "CNPJ+ANO": f"{cnpj}{year}", 
                "CNPJ": cnpj,
                "Ano": year,
                "Regime": regime,
                "": "",  # coluna vazia no Excel
                "Motivo": motivo
            })

        time.sleep(SLEEP)  # dá uma respirada entre as consultas

    # joga tudo no Excel
    df = pd.DataFrame(rows, columns=["CNPJ+ANO", "CNPJ", "Ano", "Regime", "", "Motivo"])
    df.to_excel(OUTPUT_FILE, index=False)


# executa só se rodar diretamente (não ao importar)
if __name__ == "__main__":
    main()

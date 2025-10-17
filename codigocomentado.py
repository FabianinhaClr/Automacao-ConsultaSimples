import os        # mexe com arquivos, pastas e variáveis do sistema
import re        # expressões regulares, pra limpar CNPJs ou buscar padrões
import time      # pra colocar pausas entre requisições (não travar a API)
from datetime import datetime, date  # pra lidar com datas, comparar períodos etc
from dotenv import load_dotenv       # carrega configs do .env (API_URL, API_KEY)
import requests   # biblioteca pra fazer requisições HTTP (API)
import pandas as pd  # organizar resultados e exportar pra Excel
from tqdm import tqdm  # barra de progresso legal pra loops longos
from pathlib import Path  # lidar com arquivos de forma moderna
from dateutil import parser as dparser  # parser esperto pra datas que vêm esquisitas

# ===============================
# CONFIGURAÇÃO
# ===============================
load_dotenv()  # carrega variáveis do .env
API_URL = os.getenv("API_URL")   # endpoint da API, se tiver
API_KEY = os.getenv("API_KEY")   # token da API

INPUT_FILE = "cnpjs.txt"         # arquivo com os CNPJs, 1 por linha
OUTPUT_FILE = "fennerPT7.xlsx"   # planilha final
YEARS = list(range(2020, 2026))  # anos que queremos checar
SLEEP = 0.5                       # pausa entre consultas
DEBUG = False                      # True pra prints detalhados no terminal

# ===============================
# FUNÇÕES DE UTILIDADE
# ===============================

def limpar_cnpj(s):
    """Recebe qualquer string e transforma em CNPJ puro, 14 dígitos"""
    return re.sub(r'\D', '', str(s)).zfill(14)


def ler_cnpjs(caminho):
    """Lê o arquivo de CNPJs e limpa cada linha"""
    p = Path(caminho)
    if not p.exists():
        raise FileNotFoundError(f"{caminho} não encontrado. Crie com 1 CNPJ por linha.")
    with open(p, "r", encoding="utf-8") as f:
        return [limpar_cnpj(line.strip()) for line in f if line.strip()]


def consultar_infosimples(cnpj):
    """
    Consulta a API ou fallback para receitaws se API_URL não estiver definido.
    Retorna: (status_code, json ou None)
    """
    try:
        if API_URL and API_URL.strip():
            # consulta via API própria
            args = {"cnpj": cnpj, "token": API_KEY, "timeout": 300}
            r = requests.post(API_URL, data=args, timeout=60)
        else:
            # fallback pra receitaws, sem token
            r = requests.get(f"https://www.receitaws.com.br/v1/cnpj/{cnpj}", timeout=60)
        try:
            j = r.json()  # tenta transformar a resposta em JSON
        except Exception:
            j = None
        return r.status_code, j
    except Exception as e:
        if DEBUG:
            print(f"[DEBUG] Erro na requisição para {cnpj}: {e}")
        return None, None


def parsear_data(s):
    """
    Tenta converter qualquer string ou formato em data.
    Primeiro tenta formatos comuns, depois usa parser esperto.
    """
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


# ===============================
# EXTRAÇÃO DE PERIODOS
# ===============================

def _pegar_valor(item, chaves):
    """Tenta várias chaves diferentes e pega o primeiro valor não vazio"""
    for k in chaves:
        if isinstance(item, dict) and k in item and item[k] not in (None, ""):
            return item[k]
    return None


def extrair_periodos_da_resposta(resp_json):
    """
    Retorna lista de dicts {'start': date, 'end': date ou None, 'detalhe': str}
    Não descarta períodos com 'excluída', considera até a data final.
    """
    periods = []
    if not resp_json or not isinstance(resp_json, dict):
        return periods

    # tenta pegar o bloco "data" da resposta
    data = resp_json.get("data")
    if isinstance(data, list) and data:
        data_root = data[0]
    elif isinstance(data, dict):
        data_root = data
    else:
        data_root = resp_json  # fallback pra varredura geral

    # possíveis chaves onde os períodos podem estar
    candidate_list_keys = [
        "simples_nacional_periodos_anteriores",
        "simples_nacional_periodos",
        "periodos_simples",
        "simples_periodos",
        "simples_nacional",
        "periodos",
        "permanencia",
        "periodo"
    ]

    # procura direto nas chaves conhecidas
    for k in candidate_list_keys:
        lst = data_root.get(k) if isinstance(data_root, dict) else None
        if isinstance(lst, list):
            for item in lst:
                if not isinstance(item, dict):
                    continue
                s = _pegar_valor(item, ["inicio_data", "data_inicio", "inicio", "data"])
                e = _pegar_valor(item, ["fim_data", "data_fim", "fim", "data_fim"])
                detalhe = _pegar_valor(item, ["detalhamento", "detalhe", "motivo"]) or ""
                si = parsear_data(s)
                ei = parsear_data(e)
                if si:
                    periods.append({"start": si, "end": ei, "detalhe": detalhe})
            if periods:
                return periods

    # fallback: vasculhar recursivamente listas de dicts
    def encontrar_listas(obj):
        found = []
        if isinstance(obj, dict):
            for v in obj.values():
                found += encontrar_listas(v)
        elif isinstance(obj, list):
            if obj and isinstance(obj[0], dict):
                found.append(obj)
            else:
                for v in obj:
                    found += encontrar_listas(v)
        return found

    listas = encontrar_listas(resp_json)
    for lst in listas:
        for item in lst:
            if not isinstance(item, dict):
                continue
            s = _pegar_valor(item, ["inicio_data", "data_inicio", "inicio", "data"])
            e = _pegar_valor(item, ["fim_data", "data_fim", "fim", "data_fim"])
            detalhe = _pegar_valor(item, ["detalhamento", "detalhe", "motivo"]) or ""
            si = parsear_data(s)
            ei = parsear_data(e)
            if si:
                periods.append({"start": si, "end": ei, "detalhe": detalhe})
    return periods


def periodos_para_string(periods):
    """Transforma a lista de períodos em uma string legível"""
    parts = []
    for p in periods:
        s = p.get("start").isoformat() if p.get("start") else ""
        e = p.get("end").isoformat() if p.get("end") else "até hoje"
        det = p.get("detalhe") or ""
        parts.append(f"{s} - {e} [{det}]")
    return "; ".join(parts)


# ===============================
# REGRAS DE CLASSIFICAÇÃO
# ===============================

def cobre_ano_com_regras(periods, year, consulta_date=None):
    """
    Checa se o ano está coberto pelos períodos.
    - anos passados: precisa cobrir 01/01 a 31/12
    - ano atual: precisa cobrir 01/01 até data da consulta
    Retorna: (bool, motivo)
    """
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
                dias = (consulta_date - year_start).days + 1
                motivos.append(f"cobre_{year}_de_01_01_ate_{consulta_date.isoformat()} ({dias} dias) [{detalhe}]")
                return True, "; ".join(motivos)
            else:
                motivos.append(f"periodo_nao_cobre_ano_atual: {si.isoformat()} - {ei_eff.isoformat()} [{detalhe}]")
                continue

        if si <= year_start and ei_eff >= year_end:
            dias = (year_end - year_start).days + 1
            motivos.append(f"cobre_{year}_inteiro: {si.isoformat()} - {ei_eff.isoformat()} ({dias} dias) [{detalhe}]")
            return True, "; ".join(motivos)

    return False, ("nao_cobre_periodo_exigido; " + "; ".join(motivos)) if motivos else ("nenhum_periodo_encontrado")


def main():
    cnpjs = ler_cnpjs(INPUT_FILE)
    rows = []

    for cnpj in tqdm(cnpjs, desc="Consultando CNPJs"):
        status, resp_json = consultar_infosimples(cnpj)

        if status is None or resp_json is None:
            # erro de conexão ou API fora do ar
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

        # se API retorna code != 200
        code = resp_json.get("code") if isinstance(resp_json, dict) else None
        if code is not None and code != 200:
            msg = resp_json.get("code_message", "sem mensagem")
            for year in YEARS:
                rows.append({
                    "CNPJ+ANO": f"{cnpj}/{year}",
                    "CNPJ": cnpj,
                    "Ano": year,
                    "Regime": f"API_CODE_{code}",
                    "Motivo": msg,
                    "Períodos_detectados": ""
                })
            continue

        # pega item de data
        data_item = None
        if isinstance(resp_json.get("data"), list) and resp_json["data"]:
            data_item = resp_json["data"][0]
        elif isinstance(resp_json.get("data"), dict):
            data_item = resp_json["data"]
        else:
            data_item = resp_json

        situacao_atual = None
        if isinstance(data_item, dict):
            # tenta vários campos possíveis da resposta
            situacao_atual = data_item.get("simples_nacional_situacao") or data_item.get("situacao_simples") or data_item.get("situacao")

        # extrai períodos da API
        periods = extrair_periodos_da_resposta(resp_json)

        # fallback: tenta montar período a partir da string situacao_atual
        if (not periods) and situacao_atual and "não optante" not in str(situacao_atual).lower():
            m = re.search(r"desde (\d{2}/\d{2}/\d{4})", str(situacao_atual))
            if m:
                start = parsear_data(m.group(1))
                if start:
                    periods.append({"start": start, "end": None, "detalhe": "situação_atual"})

        periods_str = periodos_para_string(periods) if periods else ""

        if DEBUG:
            print(f"\n[DEBUG] CNPJ: {cnpj}")
            print("  situacao_atual:", situacao_atual)
            print("  periods_detected:", periods_str)

        # aplica regra pra cada ano
        for year in YEARS:
            is_opt, motivo = cobre_ano_com_regras(periods, year, consulta_date=date.today())
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

        time.sleep(SLEEP)  # respira um pouco entre consultas

    # salva tudo no Excel
    df = pd.DataFrame(rows, columns=["CNPJ+ANO", "CNPJ", "Ano", "Regime", "Motivo", "Períodos_detectados", "Situacao_Atual"])
    df.to_excel(OUTPUT_FILE, index=False)
    print(f"\n✅ Consulta finalizada. Planilha salva em {OUTPUT_FILE}")


if __name__ == "__main__":
    main()


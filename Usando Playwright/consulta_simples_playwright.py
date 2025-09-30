import asyncio
from playwright.async_api import async_playwright
import pandas as pd
from pathlib import Path

INPUT_FILE = "cnpj.txt"
OUTPUT_FILE = "resultado_playwright.xlsx"

async def consultar_cnpj(page, cnpj: str) -> str:
    await page.goto("https://consopt.www8.receita.fazenda.gov.br/consultaoptantes")

    await page.fill("input[name='Cnpj']", cnpj)

    await page.click("input[type='submit'][value='Consultar']")

    await page.wait_for_selector("table, .msgErro, .tabelaSimples", timeout=60000)

    return await page.content()

async def main():
    cnpjs = []
    p = Path(INPUT_FILE)
    if p.exists():
        with open(p, "r", encoding="utf-8") as f:
            cnpjs = [line.strip() for line in f if line.strip()]

    if not cnpjs:
        print("⚠ Nenhum CNPJ encontrado no arquivo autocnpjs.txt")
        return

    resultados = []
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False, slow_mo=500)
        page = await browser.new_page()

        for cnpj in cnpjs:
            print(f"🔎 Consultando {cnpj}...")
            try:
                content = await consultar_cnpj(page, cnpj)
                resultados.append({"CNPJ": cnpj, "Resultado_HTML": content[:1000]})
            except Exception as e:
                print(f"❌ Erro consultando {cnpj}: {e}")
                resultados.append({"CNPJ": cnpj, "Resultado_HTML": f"ERRO: {e}"})

        await browser.close()

    df = pd.DataFrame(resultados)
    df.to_excel(OUTPUT_FILE, index=False)
    print(f"\n✅ Resultados salvos em {OUTPUT_FILE}")

if __name__ == "__main__":
    asyncio.run(main())

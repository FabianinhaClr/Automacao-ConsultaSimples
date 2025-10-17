# -*- coding: utf-8 -*-
# Tkinter + página web local p/ upload e processamento de planilha (.xlsx/.xlsm)
# Sem frameworks. Usa http.server (padrão do Python) e seu script original (main()).

import os
import importlib.util
import threading
import tkinter as tk
from tkinter import filedialog, messagebox
import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
import tempfile
from email.parser import BytesParser
from email.policy import default

# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# ALTERE AQUI: nome do seu script original (no mesmo diretório) que possui main()
SCRIPT_FILENAME = "seu_script.py"   # ex.: "consulta_sn.py"
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

APP_TITLE = "Consulta Simples Nacional"
PHRASE = "Envie sua planilha e eu faço o resto"

# ------------ Loader do módulo do usuário (exige função main()) ------------
def load_user_module():
    spec = importlib.util.spec_from_file_location("usercode", SCRIPT_FILENAME)
    if spec is None or spec.loader is None:
        raise RuntimeError(
            f"Não consegui carregar o script '{SCRIPT_FILENAME}'. "
            f"Confirme o nome do arquivo e se ele está na mesma pasta do ui_tk_min.py."
        )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    if not hasattr(mod, "main"):
        raise RuntimeError("O script original não possui a função main().")
    return mod

# -------------------- Ação do botão desktop (original) --------------------
def select_and_run():
    path = filedialog.askopenfilename(
        title="Selecione a planilha (.xlsx)",
        filetypes=[("Excel", "*.xlsx *.xlsm")]
    )
    if not path:
        return

    def worker():
        try:
            mod = load_user_module()
            setattr(mod, "INPUT_FILE", path)
            btn_local.config(state=tk.DISABLED)
            root.config(cursor="watch")
            root.update_idletasks()

            mod.main()

            root.after(0, lambda: messagebox.showinfo(
                "Pronto",
                "A consulta foi finalizada e a aba 'CONSULTA' foi adicionada na planilha selecionada."
            ))
        except Exception as e:
            root.after(0, lambda: messagebox.showerror("Erro", f"Ocorreu um erro: {e}"))
        finally:
            root.after(0, lambda: (btn_local.config(state=tk.NORMAL), root.config(cursor="")))

    threading.Thread(target=worker, daemon=True).start()

# ------------------ Página web de upload (servida localmente) ------------------

# HTML simples com formulário de upload (abre em nova aba; resposta é download)
UPLOAD_HTML = """<!doctype html>
<html lang="pt-br">
<head>
<meta charset="utf-8">
<title>Upload – Consulta Simples Nacional</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  :root { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; }
  body { margin: 32px; max-width: 880px; }
  h1 { font-size: 1.4rem; margin-bottom: 8px; }
  p.hint { color: #555; margin-top: 0; }
  .card { border: 1px solid #e5e5e5; border-radius: 10px; padding: 24px; }
  .drop { border: 2px dashed #bbb; border-radius: 12px; padding: 28px; text-align: center; }
  .drop.drag { border-color: #0d6efd; background: #f7fbff; }
  .actions { margin-top: 16px; display: flex; gap: 12px; align-items: center; }
  button { padding: 10px 16px; border: 1px solid #0d6efd; background: #0d6efd; color: white; border-radius: 8px; cursor: pointer; }
  button:disabled { opacity: .6; cursor: default; }
  input[type=file] { display: none; }
  .name { color:#333; font-weight:500; }
  .small { color:#666; font-size:.92rem; }
  footer { margin-top: 28px; color:#666; font-size:.9rem; }
</style>
</head>
<body>
  <h1>Enviar planilha para consulta</h1>
  <p class="hint">A planilha será processada localmente por este computador e o arquivo resultante será baixado automaticamente.</p>

  <div class="card">
    <div id="drop" class="drop">
      <p class="small">Arraste e solte aqui, ou</p>
      <label for="file"><button type="button">Escolher planilha (.xlsx ou .xlsm)</button></label>
      <form id="form" method="POST" action="/upload" enctype="multipart/form-data" target="_blank">
        <input id="file" type="file" name="file" accept=".xlsx,.xlsm" required>
        <div class="actions">
          <span id="filename" class="name"></span>
          <button id="send" type="submit" disabled>Enviar</button>
          <span id="status" class="small"></span>
        </div>
      </form>
    </div>
  </div>

  <footer>Servidor local • Se esta página foi aberta por engano, você pode fechá-la com segurança.</footer>

<script>
  const fileInput = document.getElementById('file');
  const sendBtn = document.getElementById('send');
  const fname = document.getElementById('filename');
  const status = document.getElementById('status');
  const drop = document.getElementById('drop');

  function setFile(f) {
    if (!f) { fname.textContent = ''; sendBtn.disabled = true; return; }
    fname.textContent = f.name;
    sendBtn.disabled = false;
  }

  fileInput.addEventListener('change', (e) => setFile(e.target.files[0]));
  document.addEventListener('dragover', (e) => { e.preventDefault(); drop.classList.add('drag'); });
  document.addEventListener('dragleave', (e) => { drop.classList.remove('drag'); });
  document.addEventListener('drop', (e) => {
    e.preventDefault(); drop.classList.remove('drag');
    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      fileInput.files = e.dataTransfer.files;
      setFile(fileInput.files[0]);
    }
  });

  document.getElementById('form').addEventListener('submit', () => {
    status.textContent = 'Enviando e processando... o download iniciará ao finalizar.';
    sendBtn.disabled = true;
  });
</script>
</body>
</html>
"""

def _content_type_for(ext: str) -> str:
    ext = ext.lower()
    if ext == ".xlsx":
        return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    if ext == ".xlsm":
        return "application/vnd.ms-excel.sheet.macroEnabled.12"
    return "application/octet-stream"


class UploadHandler(BaseHTTPRequestHandler):
    # Evita logs muito verbosos no console
    def log_message(self, fmt, *args):
        return

    def do_GET(self):
        if self.path in ("/", "/index", "/index.html"):
            page = UPLOAD_HTML.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(page)))
            self.end_headers()
            self.wfile.write(page)
        else:
            self.send_error(404, "Not found")

    def do_POST(self):
        if self.path != "/upload":
            self.send_error(404, "Not found")
            return

        ctype = self.headers.get("Content-Type", "")
        if "multipart/form-data" not in ctype:
            self.send_error(400, "Conteúdo inválido (esperado multipart/form-data)")
            return

        # Tamanho do corpo
        try:
            clen = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self.send_error(411, "Content-Length ausente/ inválido")
            return
        if clen <= 0:
            self.send_error(400, "Request body vazio")
            return

        try:
            # Lê o corpo inteiro (ok para .xlsx/.xlsm típicos)
            body = self.rfile.read(clen)

            # Monta uma mensagem MIME com o cabeçalho Content-Type original
            raw = b"Content-Type: " + ctype.encode("utf-8") + b"\r\nMIME-Version: 1.0\r\n\r\n" + body
            msg = BytesParser(policy=default).parsebytes(raw)

            # Encontra a parte do formulário com name="file"
            file_bytes = None
            filename = None

            if msg.is_multipart():
                for part in msg.iter_parts():
                    cd = part.get("Content-Disposition", "")
                    if cd and "form-data" in cd:
                        name = part.get_param("name", header="Content-Disposition")
                        fn = part.get_param("filename", header="Content-Disposition")
                        if name == "file" and fn:
                            filename = fn
                            file_bytes = part.get_payload(decode=True)
                            break

            if not filename or file_bytes is None:
                self.send_error(400, "Arquivo não enviado")
                return

            ext = Path(filename).suffix or ".xlsx"
            if ext.lower() not in (".xlsx", ".xlsm"):
                self.send_error(400, "Tipo de arquivo não suportado. Envie .xlsx ou .xlsm.")
                return

            # Salva em arquivo temporário
            with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
                temp_path = tmp.name
                tmp.write(file_bytes)

            # Processa com o script do usuário
            mod = load_user_module()
            setattr(mod, "INPUT_FILE", temp_path)
            mod.main()  # deve atualizar o próprio arquivo temp (aba "CONSULTA")

            # Responde com download do arquivo processado
            out_name = f"{Path(filename).stem}_CONSULTA{ext}"
            ctype_out = _content_type_for(ext)

            self.send_response(200)
            self.send_header("Content-Type", ctype_out)
            self.send_header("Content-Disposition", f'attachment; filename="{out_name}"')
            self.end_headers()

            with open(temp_path, "rb") as f:
                while True:
                    data = f.read(1024 * 1024)
                    if not data:
                        break
                    self.wfile.write(data)

        except Exception as e:
            self.send_response(500)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(f"<h1>Erro</h1><pre>{e}</pre>".encode("utf-8"))
        finally:
            # Remove o temporário
            try:
                if "temp_path" in locals() and os.path.exists(temp_path):
                    os.unlink(temp_path)
            except Exception:
                pass

# --------- Inicialização da janela principal (Tkinter) ----------
root = tk.Tk()
root.title(APP_TITLE)
root.resizable(False, False)

def center(win, w=520, h=200):
    win.update_idletasks()
    sw = win.winfo_screenwidth()
    sh = win.winfo_screenheight()
    x = int((sw - w) / 2)
    y = int((sh - h) / 2)
    win.geometry(f"{w}x{h}+{x}+{y}")

lbl = tk.Label(root, text=PHRASE, font=("Segoe UI", 14))
lbl.pack(padx=24, pady=(24, 12))

# Botão fluxo desktop (original)
btn_local = tk.Button(root, text="Selecionar planilha (.xlsx)", command=select_and_run, width=28)
btn_local.pack(pady=(0, 8))

# ---- Controles do servidor web local ----
httpd = None

def start_local_upload_page():
    global httpd
    if httpd is not None:
        # Já está rodando: só abre/foi aberto
        webbrowser.open_new_tab(f"http://127.0.0.1:{httpd.server_port}/")
        return

    # Tenta porta 8765; se estiver ocupada, usa aleatória (0)
    for port in (8765, 0):
        try:
            httpd = HTTPServer(("127.0.0.1", port), UploadHandler)
            break
        except OSError:
            httpd = None
            continue

    if httpd is None:
        messagebox.showerror("Erro", "Não consegui iniciar o servidor local.")
        return

    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    webbrowser.open_new_tab(f"http://127.0.0.1:{httpd.server_port}/")
    messagebox.showinfo("Servidor Web", f"Página de upload aberta em http://127.0.0.1:{httpd.server_port}/")

def stop_local_upload_page():
    global httpd
    if httpd is not None:
        httpd.shutdown()
        httpd.server_close()
        httpd = None
        messagebox.showinfo("Servidor Web", "Servidor finalizado.")

btn_web_start = tk.Button(root, text="Abrir página web (upload)", command=start_local_upload_page, width=28)
btn_web_start.pack(pady=(0, 0))

btn_web_stop = tk.Button(root, text="Parar página web", command=stop_local_upload_page, width=28)
btn_web_stop.pack(pady=(6, 16))

def on_close():
    try:
        stop_local_upload_page()
    except Exception:
        pass
    root.destroy()

root.protocol("WM_DELETE_WINDOW", on_close)

center(root)
root.mainloop()

"""
comparativo_sincrono/robo_api.py - o MESMO robô, mas como uma API HTTP síncrona.

Serve de BASE DE COMPARAÇÃO com a versão Kafka: não usa Kafka, só a
biblioteca padrão do Python. A regra de decisão é exatamente a mesma
(dominio/estrategia.py); o que muda é apenas a forma de integração:
aqui a bolsa precisa chamar o robô diretamente e esperar a resposta.

Uso (a partir da pasta raiz do projeto)
  python comparativo_sincrono/robo_api.py                              (porta 8001)
  python comparativo_sincrono/robo_api.py --atraso 0.5                 (robô lento)
  python comparativo_sincrono/robo_api.py --porta 8002 --nome auditoria (2º "consumidor")
"""
import argparse
import json
import sys
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

# Permite importar os módulos da pasta raiz (dominio/, infra/).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dominio.estrategia import EstrategiaMediaMovel  # noqa: E402
from infra.terminal import Cor, brl, colorir  # noqa: E402

CORES_ACAO = {"COMPRAR": Cor.VERDE, "VENDER": Cor.VERMELHO, "MANTER": Cor.CINZA, "AGUARDANDO": Cor.CINZA}


def criar_servidor(porta: int, nome: str, atraso: float) -> HTTPServer:
    estrategia = EstrategiaMediaMovel(janela=5, limiar=0.002)

    class Manipulador(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802 (nome exigido pelo http.server)
            if self.path != "/cotacoes":
                self.send_error(404, "use POST /cotacoes")
                return
            tamanho = int(self.headers.get("Content-Length", 0))
            try:
                evento = json.loads(self.rfile.read(tamanho))
                ativo, preco = evento["ativo"], float(evento["preco"])
            except (ValueError, KeyError, TypeError):
                self.send_error(400, "evento fora do contrato")
                return

            if atraso:
                time.sleep(atraso)  # processamento lento: QUEM CHAMOU fica esperando
            decisao = estrategia.avaliar(ativo, preco)
            print(f"[{nome} HTTP] {ativo:<6} {brl(preco):>10} | "
                  f"{colorir(decisao.acao, CORES_ACAO[decisao.acao])}")

            corpo = json.dumps({"acao": decisao.acao}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(corpo)))
            self.end_headers()
            self.wfile.write(corpo)

        def log_message(self, *args) -> None:
            pass  # silencia o log padrão do http.server (já imprimimos o nosso)

    # 127.0.0.1 em vez de "localhost": no Windows, "localhost" tenta IPv6
    # primeiro e isso adicionaria ~2 s falsos de latência em cada chamada.
    return HTTPServer(("127.0.0.1", porta), Manipulador)


def main() -> None:
    parser = argparse.ArgumentParser(description="Comparativo: robô exposto como API HTTP síncrona.")
    parser.add_argument("--porta", type=int, default=8001)
    parser.add_argument("--nome", default="robô")
    parser.add_argument("--atraso", type=float, default=0.0, help="segundos de processamento por cotação")
    args = parser.parse_args()

    servidor = criar_servidor(args.porta, args.nome, args.atraso)
    print(colorir(f"[{args.nome} HTTP] ouvindo em http://127.0.0.1:{args.porta}/cotacoes"
                  + (f" (atraso de {args.atraso}s por chamada)" if args.atraso else "")
                  + ". Ctrl+C para parar.", Cor.NEGRITO))
    try:
        servidor.serve_forever()
    except KeyboardInterrupt:
        print(f"\n[{args.nome} HTTP] desligado.")
    finally:
        servidor.server_close()


if __name__ == "__main__":
    main()

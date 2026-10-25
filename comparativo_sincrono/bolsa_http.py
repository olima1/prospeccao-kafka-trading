"""
comparativo_sincrono/bolsa_http.py - a MESMA bolsa simulada, mas chamando cada
consumidor DIRETAMENTE por HTTP (integração síncrona ponto a ponto). Sem Kafka.

O que este script deixa evidente (compare com bolsa_simulada.py):
  1. Acoplamento de LOCALIZAÇÃO: a bolsa precisa saber o endereço de cada
     consumidor (--destinos). Consumidor novo = mudar e reiniciar a bolsa.
  2. Acoplamento TEMPORAL: se o robô estiver desligado, a chamada falha e a
     cotação se perde - a não ser que a bolsa implemente fila, retentativa e
     armazenamento por conta própria (ou seja, que reinvente um broker).
  3. Acoplamento de DESEMPENHO: a bolsa espera cada resposta, então um robô
     lento deixa a bolsa lenta.

Uso (a partir da pasta raiz do projeto)
  python comparativo_sincrono/bolsa_http.py
  python comparativo_sincrono/bolsa_http.py --destinos http://127.0.0.1:8001/cotacoes,http://127.0.0.1:8002/cotacoes
"""
import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

# Permite importar os módulos da pasta raiz (dominio/, infra/).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dominio.mercado import ATIVOS_PADRAO, MercadoSimulado  # noqa: E402
from infra.terminal import Cor, colorir, numero, segundos  # noqa: E402


def enviar(url: str, evento: dict, timeout: float) -> None:
    """POST síncrono: só retorna depois que o consumidor terminar e responder."""
    requisicao = urllib.request.Request(
        url,
        data=json.dumps(evento).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(requisicao, timeout=timeout) as resposta:
        resposta.read()


def main() -> None:
    parser = argparse.ArgumentParser(description="Comparativo: bolsa chamando os consumidores via HTTP.")
    parser.add_argument("--destinos", default="http://127.0.0.1:8001/cotacoes",
                        help="URLs dos consumidores, separadas por vírgula (padrão: %(default)s)")
    parser.add_argument("--ativos", default=",".join(ATIVOS_PADRAO))
    parser.add_argument("--intervalo", type=float, default=1.0, help="meta de segundos por rodada")
    parser.add_argument("--timeout", type=float, default=5.0, help="tempo máximo de espera por resposta")
    args = parser.parse_args()

    destinos = [d.strip() for d in args.destinos.split(",") if d.strip()]
    mercado = MercadoSimulado([a.strip().upper() for a in args.ativos.split(",") if a.strip()])
    print(colorir(f"[bolsa HTTP] chamando diretamente: {', '.join(destinos)}. Ctrl+C para parar.", Cor.NEGRITO))

    rodada = entregues = perdidas = 0
    try:
        while True:
            rodada += 1
            inicio = time.monotonic()
            latencias = []
            for ativo in mercado.ativos:
                evento = mercado.gerar_evento(ativo)
                for url in destinos:  # a bolsa precisa conhecer CADA consumidor
                    t0 = time.monotonic()
                    try:
                        enviar(url, evento, args.timeout)
                        latencias.append(time.monotonic() - t0)
                        entregues += 1
                    except (urllib.error.URLError, OSError) as erro:
                        perdidas += 1
                        motivo = getattr(erro, "reason", erro)
                        print(colorir(f"[bolsa HTTP] FALHA {ativo} -> {url}: {motivo}  (cotação PERDIDA)",
                                      Cor.VERMELHO))

            duracao = time.monotonic() - inicio
            media_ms = 1000 * sum(latencias) / len(latencias) if latencias else 0
            atrasada = duracao > args.intervalo * 1.2
            print(colorir(
                f"[bolsa HTTP] rodada {rodada}: latência média {media_ms:.0f} ms | a rodada levou "
                f"{segundos(duracao)} (meta {segundos(args.intervalo)}) | entregues {numero(entregues)} | "
                f"perdidas {numero(perdidas)}",
                Cor.VERMELHO if (atrasada or perdidas) else Cor.CINZA,
            ))
            time.sleep(max(0.0, args.intervalo - duracao))
    except KeyboardInterrupt:
        print(f"\n[bolsa HTTP] encerrada. Entregues: {numero(entregues)} | perdidas: {numero(perdidas)}")


if __name__ == "__main__":
    main()

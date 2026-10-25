"""
robo_trading.py - CONSUMIDOR 1: robô que registra COMPRAR / VENDER / MANTER.

Lê o tópico "cotacoes" em tempo real e registra uma decisão no console.
A regra fica em dominio/estrategia.py e é propositalmente simples: o foco
da PoC é a arquitetura orientada a eventos, não a estratégia.

Conceitos demonstrados
  * Consumer group: várias instâncias com o mesmo --grupo DIVIDEM as
    partições entre si (escala horizontal). Quando uma instância entra ou
    sai, o Kafka redistribui as partições sozinho (rebalanceamento).
  * Offset confirmado manualmente (commit) DEPOIS de processar -> garantia
    "at-least-once": desligou e religou, o robô continua de onde parou.
  * --atraso simula um processamento lento: o produtor não é afetado; as
    cotações se acumulam no Kafka (lag) até o robô dar conta.

Uso
  python robo_trading.py --instancia A
  python robo_trading.py --instancia B               (mesmo grupo: divide as partições)
  python robo_trading.py --instancia A --atraso 0.5  (robô lento)
"""
import argparse
import json
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaException

from dominio.estrategia import EstrategiaMediaMovel
from infra import config_kafka
from infra.terminal import Cor, brl, colorir, rotulo_particao, segundos

CORES_ACAO = {
    "COMPRAR": Cor.VERDE,
    "VENDER": Cor.VERMELHO,
    "MANTER": Cor.CINZA,
    "AGUARDANDO": Cor.CINZA,
}


def ler_cotacao(valor: bytes) -> tuple[str, float, dict]:
    """Extrai do evento SÓ o que o robô precisa (padrão "leitor tolerante").

    Campos extras - como o "volume" da versão 2 - são ignorados, então o
    produtor pode evoluir o contrato sem quebrar este consumidor.
    """
    evento = json.loads(valor)
    return evento["ativo"], float(evento["preco"]), evento


def idade_do_evento(evento: dict) -> float | None:
    """Há quantos segundos o evento foi emitido pela bolsa."""
    try:
        emitido_em = datetime.fromisoformat(evento["emitido_em"])
    except (KeyError, TypeError, ValueError):
        return None
    return max(0.0, (datetime.now(timezone.utc) - emitido_em).total_seconds())


def main() -> None:
    parser = argparse.ArgumentParser(description="Consumidor 1: robô de decisão (didático).")
    parser.add_argument("--instancia", default="A", help="nome desta instância nos logs (padrão: A)")
    parser.add_argument("--grupo", default="robo-trading", help="consumer group (padrão: robo-trading)")
    parser.add_argument("--atraso", type=float, default=0.0,
                        help="segundos de 'processamento' por cotação, para simular um robô lento")
    args = parser.parse_args()
    nome = f"[robô {args.instancia}]"

    config_kafka.garantir_topico()

    consumidor = Consumer({
        **config_kafka.config_base(f"robo-{args.instancia}"),
        "group.id": args.grupo,           # instâncias com o mesmo group.id dividem o trabalho
        "auto.offset.reset": "latest",    # 1ª vez do grupo: começa pelas cotações novas
        "enable.auto.commit": False,      # o commit é feito por nós, depois de processar
    })

    # Partições em que já mostramos "lendo a partir do offset X" desde a última atribuição.
    particoes_anunciadas: set[int] = set()

    def ao_receber_particoes(_consumidor, particoes) -> None:
        particoes_anunciadas.clear()
        lista = ", ".join(f"p{p.partition}" for p in particoes) or "nenhuma (há mais instâncias que partições)"
        print(colorir(f"{nome} partições atribuídas a esta instância: {lista}", Cor.AMARELO))

    def ao_perder_particoes(_consumidor, particoes) -> None:
        if particoes:
            lista = ", ".join(f"p{p.partition}" for p in particoes)
            print(colorir(f"{nome} rebalanceamento: devolvendo {lista}", Cor.AMARELO))

    consumidor.subscribe(
        [config_kafka.TOPICO_COTACOES],
        on_assign=ao_receber_particoes,
        on_revoke=ao_perder_particoes,
    )
    estrategia = EstrategiaMediaMovel(janela=5, limiar=0.002)
    print(colorir(
        f"{nome} grupo '{args.grupo}' ouvindo '{config_kafka.TOPICO_COTACOES}'"
        + (f" (atraso simulado de {args.atraso}s por cotação)" if args.atraso else "")
        + ". Ctrl+C para parar.",
        Cor.NEGRITO,
    ))

    try:
        while True:
            mensagem = consumidor.poll(1.0)  # espera até 1 s por uma mensagem
            if mensagem is None:
                continue
            if mensagem.error():
                print(colorir(f"{nome} erro do Kafka: {mensagem.error()}", Cor.VERMELHO))
                continue

            particao, offset = mensagem.partition(), mensagem.offset()
            if particao not in particoes_anunciadas:
                particoes_anunciadas.add(particao)
                print(colorir(f"{nome} {rotulo_particao(particao)}: lendo a partir do offset {offset}",
                              Cor.AMARELO))

            try:
                ativo, preco, evento = ler_cotacao(mensagem.value())
            except (ValueError, KeyError, TypeError) as erro:
                # Evento fora do contrato: registra, pula e confirma para não travar a partição.
                motivo = f"falta o campo {erro}" if isinstance(erro, KeyError) else str(erro)
                print(colorir(f"{nome} evento inválido em p{particao}/offset {offset} ignorado ({motivo})",
                              Cor.VERMELHO))
                consumidor.commit(message=mensagem, asynchronous=True)
                continue

            decisao = estrategia.avaliar(ativo, preco)
            if args.atraso:
                time.sleep(args.atraso)  # "processamento" lento de propósito

            media = brl(decisao.media) if decisao.media is not None else "--"
            acao = colorir(f"{decisao.acao:<10}", CORES_ACAO[decisao.acao])
            idade = idade_do_evento(evento)
            idade_txt = "" if idade is None else f"idade {segundos(idade)}"
            if idade is not None and idade > 2:
                idade_txt = colorir(idade_txt, Cor.AMARELO)  # evento antigo: estava esperando no Kafka
            print(f"{nome} {rotulo_particao(particao)} #{offset:<5} {ativo:<6} {brl(preco):>10}"
                  f" | média {media:>10} | {acao} {idade_txt}")

            # Confirma ao Kafka que esta mensagem foi processada. Se o robô cair
            # ANTES desta linha, a mensagem será entregue de novo (at-least-once).
            consumidor.commit(message=mensagem, asynchronous=True)
    except KeyboardInterrupt:
        print(f"\n{nome} desligando...")
    finally:
        try:
            consumidor.commit(asynchronous=False)  # último commit, esperando a resposta
        except KafkaException:
            pass  # nada novo para confirmar
        consumidor.close()  # sai do grupo: o Kafka rebalanceia na hora para as outras instâncias
        print(f"{nome} desligado.")


if __name__ == "__main__":
    main()

"""
bolsa_simulada.py - PRODUTOR de eventos (simula a infraestrutura da bolsa).

Gera cotações aleatórias e publica cada uma como um EVENTO no tópico
"cotacoes" do Kafka.

O produtor NÃO conhece os consumidores: não sabe quantos são, onde rodam,
em que linguagem foram escritos, nem se estão ligados. Ele só conhece o
tópico. É isso que permite adicionar, desligar ou trocar consumidores sem
mexer nesta linha de código.

Conceitos demonstrados
  * chave da mensagem = código do ativo -> mesma chave vai sempre para a
    mesma partição -> a ORDEM das cotações de cada ativo é preservada;
  * acks=all + idempotência -> o broker confirma cada evento gravado, e as
    retentativas automáticas não geram eventos duplicados;
  * --versao 2 acrescenta o campo "volume" ao evento (evolução do contrato
    sem quebrar quem já consome a versão 1).

Uso
  python bolsa_simulada.py
  python bolsa_simulada.py --intervalo 0.5
  python bolsa_simulada.py --versao 2
"""
import argparse
import json
import time

from confluent_kafka import Producer

from dominio.mercado import ATIVOS_PADRAO, MercadoSimulado
from infra import config_kafka
from infra.terminal import Cor, brl, colorir, rotulo_particao, segundos


def ao_confirmar_entrega(erro, mensagem) -> None:
    """Callback chamado quando o broker confirma (ou recusa) cada evento."""
    if erro is not None:
        print(colorir(f"[bolsa] FALHA ao publicar {mensagem.key().decode()}: {erro}", Cor.VERMELHO))
        return
    evento = json.loads(mensagem.value())
    print(
        f"[bolsa v{evento['versao']}] {evento['ativo']:<6} {brl(evento['preco']):>10}"
        f"  -> gravado em {rotulo_particao(mensagem.partition())}, offset {mensagem.offset()}"
    )


def esperar_atendendo_callbacks(produtor: Producer, segundos: float) -> None:
    """Igual a time.sleep(segundos), mas processando as confirmações do broker
    enquanto espera (assim cada confirmação aparece no console na hora)."""
    fim = time.monotonic() + segundos
    while (restante := fim - time.monotonic()) > 0:
        produtor.poll(restante)


def main() -> None:
    parser = argparse.ArgumentParser(description="Produtor: publica cotações simuladas no Kafka.")
    parser.add_argument("--ativos", default=",".join(ATIVOS_PADRAO),
                        help="códigos separados por vírgula (padrão: %(default)s)")
    parser.add_argument("--intervalo", type=float, default=1.0,
                        help="segundos entre uma rodada de cotações e a próxima (padrão: 1.0)")
    parser.add_argument("--versao", type=int, choices=(1, 2), default=1,
                        help="versão do contrato do evento (2 = com o campo 'volume')")
    args = parser.parse_args()

    config_kafka.garantir_topico()

    produtor = Producer({
        **config_kafka.config_base("bolsa-simulada"),
        "acks": "all",                    # só conta como entregue depois de gravado no broker
        "enable.idempotence": True,       # retentativa não vira evento duplicado
        "partitioner": "murmur2_random",  # mesmo hash do cliente Java: a chave cai na mesma partição em qualquer linguagem
        "linger.ms": 5,                   # espera até 5 ms para mandar as mensagens em lote
    })

    ativos = [a.strip().upper() for a in args.ativos.split(",") if a.strip()]
    mercado = MercadoSimulado(ativos)
    print(colorir(
        f"Bolsa simulada publicando no tópico '{config_kafka.TOPICO_COTACOES}' "
        f"(contrato v{args.versao}, uma rodada a cada {segundos(args.intervalo)}). Ctrl+C para parar.",
        Cor.NEGRITO,
    ))

    try:
        while True:
            for ativo in mercado.ativos:
                evento = mercado.gerar_evento(ativo, versao=args.versao)
                # produce() é ASSÍNCRONO: coloca o evento num buffer local e volta
                # na hora; o envio ao broker acontece em segundo plano.
                produtor.produce(
                    config_kafka.TOPICO_COTACOES,
                    key=ativo,                                 # define a partição
                    value=json.dumps(evento).encode("utf-8"),  # o evento em si
                    on_delivery=ao_confirmar_entrega,
                )
            esperar_atendendo_callbacks(produtor, args.intervalo)
    except KeyboardInterrupt:
        print("\nEncerrando a bolsa...")
    finally:
        # flush() envia o que ainda estiver no buffer antes de sair.
        pendentes = produtor.flush(10)
        if pendentes:
            print(colorir(f"{pendentes} evento(s) não foram confirmados pelo broker.", Cor.AMARELO))
        print("Bolsa encerrada.")


if __name__ == "__main__":
    main()

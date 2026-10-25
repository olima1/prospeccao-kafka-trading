"""
monitor.py - painel do Kafka no terminal (atualiza a cada 2 segundos).

Não produz nem consome cotações: só pergunta ao broker (AdminClient) como
está o tópico e cada consumer group. Mostra:
  * as partições do tópico e o "fim do log" (quantas mensagens já foram gravadas);
  * cada grupo: estado, membros e qual partição está com qual instância;
  * o offset confirmado (commit) de cada partição e o LAG:

        LAG = fim do log - offset confirmado   (quantas mensagens faltam ler)

Use durante as demos: desligue o robô e veja o lag crescer; religue e veja
o lag zerar; suba uma 2ª instância e veja as partições se redistribuírem.

Uso
  python monitor.py
  python monitor.py --intervalo 1
"""
import argparse
import time
from datetime import datetime

from confluent_kafka import ConsumerGroupTopicPartitions, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec

from infra import config_kafka
from infra.terminal import Cor, colorir, numero, rotulo_particao

TOPICO = config_kafka.TOPICO_COTACOES

ESTADOS = {
    "STABLE": "STABLE (consumindo)",
    "EMPTY": "EMPTY (nenhuma instância ligada)",
    "PREPARING_REBALANCE": "REBALANCEANDO...",
    "COMPLETING_REBALANCE": "REBALANCEANDO...",
    "DEAD": "DEAD (grupo removido)",
}


def fim_do_log(admin: AdminClient, particoes: list[int]) -> dict[int, int]:
    """Offset da PRÓXIMA mensagem a ser gravada em cada partição."""
    pedido = {TopicPartition(TOPICO, p): OffsetSpec.latest() for p in particoes}
    resultado = admin.list_offsets(pedido, request_timeout=5)
    return {tp.partition: futuro.result().offset for tp, futuro in resultado.items()}


def offsets_confirmados(admin: AdminClient, grupo: str) -> dict[int, int]:
    """Último offset confirmado (commit) pelo grupo em cada partição do tópico."""
    pedido = [ConsumerGroupTopicPartitions(grupo)]
    futuro = admin.list_consumer_group_offsets(pedido, request_timeout=5)[grupo]
    return {tp.partition: tp.offset for tp in futuro.result().topic_partitions
            if tp.topic == TOPICO and tp.offset >= 0}


def coletar(admin: AdminClient) -> dict:
    """Busca no broker tudo o que o painel mostra."""
    metadados = admin.list_topics(timeout=5)
    if TOPICO not in metadados.topics:
        return {"particoes": [], "fim": {}, "grupos": []}
    particoes = sorted(metadados.topics[TOPICO].partitions)
    fim = fim_do_log(admin, particoes)

    ids = sorted(g.group_id for g in admin.list_consumer_groups(request_timeout=5).result().valid)
    descricoes = admin.describe_consumer_groups(ids, request_timeout=5) if ids else {}
    grupos = []
    for grupo in ids:
        descricao = descricoes[grupo].result()
        dono = {}  # partição -> client.id da instância que está com ela
        for membro in descricao.members:
            for tp in (membro.assignment.topic_partitions if membro.assignment else []):
                if tp.topic == TOPICO:
                    dono[tp.partition] = membro.client_id
        confirmados = offsets_confirmados(admin, grupo)
        if dono or confirmados:  # ignora grupos que não têm relação com este tópico
            grupos.append({
                "id": grupo,
                "estado": descricao.state.name,
                "membros": len(descricao.members),
                "dono": dono,
                "confirmados": confirmados,
            })
    return {"particoes": particoes, "fim": fim, "grupos": grupos}


def cor_do_lag(lag: int) -> str:
    return Cor.VERDE if lag == 0 else (Cor.AMARELO if lag < 50 else Cor.VERMELHO)


def desenhar(dados: dict) -> None:
    linhas = [
        colorir(f"MONITOR DO KAFKA  |  {config_kafka.BOOTSTRAP_SERVERS}  |  "
                f"{datetime.now():%H:%M:%S}   (Ctrl+C para sair)", Cor.NEGRITO),
        "",
    ]
    if not dados["particoes"]:
        linhas.append(f"O tópico '{TOPICO}' ainda não existe. Inicie a bolsa_simulada.py.")
        print("\033[H\033[2J" + "\n".join(linhas), flush=True)
        return

    total_log = sum(dados["fim"].values())
    linhas.append(colorir(f"Tópico '{TOPICO}': {len(dados['particoes'])} partições, "
                          f"{numero(total_log)} mensagens gravadas", Cor.CIANO))
    linhas.append("  partição   fim do log")
    for p in dados["particoes"]:
        linhas.append(f"  {rotulo_particao(p)}         {numero(dados['fim'][p]):>10}")

    if not dados["grupos"]:
        linhas += ["", "Nenhum consumer group leu este tópico ainda."]
    for grupo in dados["grupos"]:
        linhas += [
            "",
            colorir(f"Grupo '{grupo['id']}'  -  {ESTADOS.get(grupo['estado'], grupo['estado'])}"
                    f"  -  {grupo['membros']} instância(s)", Cor.NEGRITO),
            "  partição   confirmado   fim do log      LAG   instância",
        ]
        lag_total = 0
        for p in dados["particoes"]:
            fim = dados["fim"][p]
            confirmado = grupo["confirmados"].get(p)
            if confirmado is None:
                confirmado_txt, lag_txt = "-", "-"
            else:
                lag = max(fim - confirmado, 0)
                lag_total += lag
                confirmado_txt, lag_txt = numero(confirmado), colorir(f"{numero(lag):>8}", cor_do_lag(lag))
            instancia = grupo["dono"].get(p, colorir("(sem instância)", Cor.CINZA))
            linhas.append(f"  {rotulo_particao(p)}         {confirmado_txt:>10}   {numero(fim):>10} "
                          f"{lag_txt:>8}   {instancia}")
        linhas.append("  LAG total: " + colorir(numero(lag_total), cor_do_lag(lag_total)))

    # \033[H\033[2J = volta o cursor ao topo e limpa a tela (redesenha o painel)
    print("\033[H\033[2J" + "\n".join(linhas), flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Painel: partições, consumer groups, offsets e lag.")
    parser.add_argument("--intervalo", type=float, default=2.0, help="segundos entre atualizações")
    args = parser.parse_args()

    admin = AdminClient(config_kafka.config_base("monitor"))
    try:
        while True:
            try:
                desenhar(coletar(admin))
            except KafkaException as erro:
                print(colorir(f"Não foi possível consultar o Kafka: {erro}", Cor.VERMELHO))
            time.sleep(args.intervalo)
    except KeyboardInterrupt:
        print("\nMonitor encerrado.")


if __name__ == "__main__":
    main()

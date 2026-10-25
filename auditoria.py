"""
auditoria.py - CONSUMIDOR 2: trilha de auditoria de todas as cotações.

É outro serviço, com outra responsabilidade e outro consumer group: grava
cada evento exatamente como chegou num arquivo JSON Lines (um JSON por
linha), o tipo de registro que áreas de compliance costumam exigir.

Por que ele é a principal evidência de DESACOPLAMENTO
  * entra no sistema depois, sem nenhuma alteração (nem reinício) no produtor;
  * por estar em OUTRO grupo, recebe TODOS os eventos, independentemente do
    robô (publish/subscribe, ou "fan-out");
  * na primeira execução lê o histórico desde o início do tópico (replay),
    algo impossível numa integração por chamada HTTP, em que nada fica guardado;
  * entende eventos v1 e v2 (o campo "volume" é opcional).

Uso
  python auditoria.py
  python auditoria.py --grupo auditoria-2   (grupo novo: relê tudo desde o início)
"""
import argparse
import json
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, KafkaException

from infra import config_kafka
from infra.terminal import Cor, colorir, numero, rotulo_particao, segundos

INTERVALO_RESUMO = 2.0  # segundos entre um resumo e outro no console
PASTA_DADOS = Path(__file__).resolve().parent / "dados"


def montar_registro(mensagem) -> tuple[dict, dict | None]:
    """Monta a linha de auditoria: metadados do Kafka + o evento original."""
    registro = {
        "particao": mensagem.partition(),
        "offset": mensagem.offset(),
        "chave": mensagem.key().decode("utf-8") if mensagem.key() else None,
        "recebido_em": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
    }
    try:
        evento = json.loads(mensagem.value())
        registro["evento"] = evento
    except ValueError:
        # A auditoria guarda até o que veio fora do contrato.
        evento = None
        registro["evento_bruto"] = mensagem.value().decode("utf-8", errors="replace")
    return registro, evento


def main() -> None:
    parser = argparse.ArgumentParser(description="Consumidor 2: trilha de auditoria das cotações.")
    parser.add_argument("--grupo", default="auditoria", help="consumer group (padrão: auditoria)")
    parser.add_argument("--arquivo", default=None,
                        help="arquivo JSON Lines de saída (padrão: dados/<grupo>.jsonl)")
    args = parser.parse_args()
    nome = f"[auditoria '{args.grupo}']"

    config_kafka.garantir_topico()
    caminho = Path(args.arquivo) if args.arquivo else PASTA_DADOS / f"{args.grupo}.jsonl"
    caminho.parent.mkdir(parents=True, exist_ok=True)

    consumidor = Consumer({
        **config_kafka.config_base(args.grupo),
        "group.id": args.grupo,           # grupo DIFERENTE do robô -> recebe sua própria cópia de tudo
        "auto.offset.reset": "earliest",  # 1ª vez do grupo: começa do início do tópico (replay)
        "enable.auto.commit": False,      # commit só depois de gravar no arquivo
    })

    particoes_anunciadas: set[int] = set()

    def ao_receber_particoes(_consumidor, particoes) -> None:
        particoes_anunciadas.clear()
        lista = ", ".join(f"p{p.partition}" for p in particoes) or "nenhuma"
        print(colorir(f"{nome} partições atribuídas: {lista}", Cor.AMARELO))

    consumidor.subscribe([config_kafka.TOPICO_COTACOES], on_assign=ao_receber_particoes)
    try:
        exibido = caminho.resolve().relative_to(Path.cwd())
    except ValueError:
        exibido = caminho
    print(colorir(f"{nome} gravando cada evento em {exibido}. Ctrl+C para parar.", Cor.NEGRITO))

    total = 0
    novos_desde_resumo = 0
    por_versao: Counter = Counter()
    ultimo_evento: dict | None = None
    proximo_resumo = time.monotonic() + INTERVALO_RESUMO

    with caminho.open("a", encoding="utf-8") as arquivo:
        try:
            while True:
                mensagem = consumidor.poll(0.5)
                if mensagem is not None and mensagem.error():
                    print(colorir(f"{nome} erro do Kafka: {mensagem.error()}", Cor.VERMELHO))
                elif mensagem is not None:
                    particao = mensagem.partition()
                    if particao not in particoes_anunciadas:
                        particoes_anunciadas.add(particao)
                        extra = "  (início do tópico: lendo o histórico)" if mensagem.offset() == 0 else ""
                        print(colorir(f"{nome} {rotulo_particao(particao)}: lendo a partir do offset "
                                      f"{mensagem.offset()}{extra}", Cor.AMARELO))

                    registro, evento = montar_registro(mensagem)
                    arquivo.write(json.dumps(registro, ensure_ascii=False) + "\n")
                    arquivo.flush()  # garante que está no arquivo ANTES do commit
                    consumidor.commit(message=mensagem, asynchronous=True)

                    total += 1
                    novos_desde_resumo += 1
                    if evento is not None:
                        por_versao[f"v{evento.get('versao', 1)}"] += 1
                        ultimo_evento = evento

                if time.monotonic() >= proximo_resumo:
                    if novos_desde_resumo:
                        print(f"{nome} +{numero(novos_desde_resumo)} eventos gravados | total "
                              f"{numero(total)} ({resumo_versoes(por_versao)}) | {descrever(ultimo_evento)}")
                    novos_desde_resumo = 0
                    proximo_resumo = time.monotonic() + INTERVALO_RESUMO
        except KeyboardInterrupt:
            print(f"\n{nome} desligando...")
        finally:
            try:
                consumidor.commit(asynchronous=False)
            except KafkaException:
                pass  # nada novo para confirmar
            consumidor.close()
            print(f"{nome} desligado. {numero(total)} eventos gravados nesta execução.")


def resumo_versoes(por_versao: Counter) -> str:
    return ", ".join(f"{versao}: {numero(qtd)}" for versao, qtd in sorted(por_versao.items())) or "-"


def descrever(evento: dict | None) -> str:
    """Resumo do último evento: mostra se é histórico (antigo) ou tempo real."""
    if not evento:
        return ""
    texto = f"último: {evento.get('ativo')}"
    if "volume" in evento:
        texto += f" vol {numero(evento['volume'])}"
    try:
        idade = (datetime.now(timezone.utc) - datetime.fromisoformat(evento["emitido_em"])).total_seconds()
        texto += f", emitido há {segundos(idade)}"
    except (KeyError, TypeError, ValueError):
        pass
    return texto


if __name__ == "__main__":
    main()

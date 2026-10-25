"""
infra/config_kafka.py - configuração de INFRAESTRUTURA compartilhada.

Aqui não existe regra de negócio. O que o produtor e os consumidores têm em
comum é só isto:
  1. o endereço do broker Kafka;
  2. o nome do tópico;
  3. o formato JSON do evento (documentado no README, seção "Contrato do evento").

Cada serviço é um processo independente: pode ser iniciado, parado, reescrito
ou multiplicado sem que os outros fiquem sabendo.
"""
import os
import sys

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from infra.terminal import Cor, colorir

# O docker-compose.yml expõe o broker para a sua máquina em localhost:9092.
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Tópico onde a bolsa publica as cotações e de onde os consumidores leem.
TOPICO_COTACOES = os.getenv("KAFKA_TOPICO", "cotacoes")

# Com 3 partições, até 3 instâncias do MESMO grupo conseguem consumir em
# paralelo (cada partição fica com uma única instância do grupo por vez).
NUM_PARTICOES = 3


def config_base(client_id: str) -> dict:
    """Configuração mínima comum a produtor, consumidores e admin."""
    return {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        # Nome do cliente: aparece no monitor.py e no Kafka UI.
        "client.id": client_id,
        # Desliga a telemetria do cliente para o broker (KIP-714).
        # Não muda nada no funcionamento; só deixa o console mais limpo.
        "enable.metrics.push": False,
    }


def garantir_topico(topico: str = TOPICO_COTACOES, particoes: int = NUM_PARTICOES) -> None:
    """Cria o tópico se ele ainda não existir (pode ser chamada várias vezes).

    Em produção os tópicos costumam ser criados pela equipe de plataforma
    (via Terraform, GitOps...). Aqui cada serviço garante o tópico ao iniciar
    só para a demonstração não depender da ordem em que os scripts sobem.
    """
    admin = AdminClient(config_base("poc-admin"))
    try:
        metadados = admin.list_topics(timeout=10)
    except KafkaException as erro:
        print(colorir(
            f"\nNão consegui falar com o Kafka em {BOOTSTRAP_SERVERS} ({erro}).\n"
            "O container está rodando? Na pasta do projeto execute:  docker compose up -d\n",
            Cor.VERMELHO,
        ))
        sys.exit(1)

    if topico in metadados.topics:
        existentes = len(metadados.topics[topico].partitions)
        if existentes < particoes:
            print(colorir(
                f"Aviso: o tópico '{topico}' já existe com {existentes} partição(ões); a demo "
                f"de escala horizontal precisa de {particoes}. Para recomeçar do zero:\n"
                "  docker compose down   e depois   docker compose up -d",
                Cor.AMARELO,
            ))
        return

    novo_topico = NewTopic(topico, num_partitions=particoes, replication_factor=1)
    for nome, futuro in admin.create_topics([novo_topico]).items():
        try:
            futuro.result()  # espera o broker confirmar a criação
            print(colorir(f"Tópico '{nome}' criado com {particoes} partições.", Cor.CIANO))
        except KafkaException as erro:
            # Outro serviço pode ter criado o tópico no mesmo instante: tudo bem.
            if erro.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise

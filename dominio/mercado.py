"""
dominio/mercado.py - simulação (bem simplificada) de uma bolsa de valores.

Não usa Kafka e não é um modelo financeiro: os preços seguem um passeio
aleatório só para gerar um fluxo contínuo de dados para a demonstração.
O domínio financeiro é apenas o CENÁRIO; o objeto de estudo é a arquitetura.
"""
import random
import uuid
from datetime import datetime, timezone

# Preços de partida aproximados (só para os números parecerem reais).
PRECOS_INICIAIS = {
    "PETR4": 37.50,
    "WEGE3": 52.10,
    "PRIO3": 41.30,
    "RENT3": 48.90,
    "VALE3": 61.20,
    "ITUB4": 34.80,
}

# Com 3 partições e o particionador murmur2 (o mesmo do cliente Java), estes
# 6 ativos ficam 2 em cada partição:
#   p0 = PETR4, WEGE3   |   p1 = PRIO3, RENT3   |   p2 = VALE3, ITUB4
ATIVOS_PADRAO = list(PRECOS_INICIAIS)


class MercadoSimulado:
    """Guarda o último preço de cada ativo e gera o próximo evento de cotação."""

    def __init__(self, ativos: list[str], volatilidade: float = 0.003, semente: int | None = None):
        self._aleatorio = random.Random(semente)
        self._volatilidade = volatilidade
        self.precos: dict[str, float] = {}
        for ativo in ativos:
            # Ativo fora da lista? Começa com um preço qualquer entre R$ 10 e R$ 100.
            inicial = PRECOS_INICIAIS.get(ativo) or self._aleatorio.uniform(10, 100)
            self.precos[ativo] = round(inicial, 2)

    @property
    def ativos(self) -> list[str]:
        return list(self.precos)

    def proximo_preco(self, ativo: str) -> float:
        """Novo preço = preço anterior com uma variação aleatória (~0,3%)."""
        variacao = self._aleatorio.gauss(0, self._volatilidade)
        novo = max(0.01, self.precos[ativo] * (1 + variacao))
        self.precos[ativo] = round(novo, 2)
        return self.precos[ativo]

    def gerar_evento(self, ativo: str, versao: int = 1) -> dict:
        """Monta o evento de cotação seguindo o CONTRATO descrito no README."""
        evento = {
            "id_evento": str(uuid.uuid4()),
            "versao": versao,
            "ativo": ativo,
            "preco": self.proximo_preco(ativo),
            "emitido_em": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        }
        if versao >= 2:
            # Versão 2 do contrato: um campo NOVO e opcional.
            # Consumidores antigos simplesmente o ignoram (leitor tolerante).
            evento["volume"] = self._aleatorio.randint(100, 5000)
        return evento

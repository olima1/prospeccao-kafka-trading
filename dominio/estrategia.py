"""
dominio/estrategia.py - regra de decisão DIDÁTICA do robô.

Não usa Kafka nem rede: recebe (ativo, preço) e devolve uma decisão. Por isso
a mesma regra é usada tanto pelo robô via Kafka (robo_trading.py) quanto pelo
robô via HTTP (comparativo_sincrono/robo_api.py) - só a integração muda.

AVISO: isto NÃO é uma estratégia de investimento. Seguindo a orientação da
avaliação, o domínio financeiro serve só de cenário para a demonstração.

Regra: compara o preço atual com a média dos últimos N preços do mesmo ativo.
    preço acima da média + limiar   ->  VENDER   (subiu "demais")
    preço abaixo da média - limiar  ->  COMPRAR  (caiu "demais")
    caso contrário                  ->  MANTER
Enquanto não houver N preços do ativo, a decisão é AGUARDANDO.
"""
from collections import defaultdict, deque
from dataclasses import dataclass


@dataclass(frozen=True)
class Decisao:
    acao: str              # "COMPRAR", "VENDER", "MANTER" ou "AGUARDANDO"
    media: float | None    # média móvel usada para decidir (None se AGUARDANDO)


class EstrategiaMediaMovel:
    def __init__(self, janela: int = 5, limiar: float = 0.002):
        self.janela = janela      # quantos preços entram na média
        self.limiar = limiar      # 0.002 = 0,2% de distância da média
        # Histórico SEPARADO por ativo; deque(maxlen) descarta o mais antigo.
        self._historico: dict[str, deque] = defaultdict(lambda: deque(maxlen=self.janela))

    def avaliar(self, ativo: str, preco: float) -> Decisao:
        historico = self._historico[ativo]
        historico.append(preco)
        if len(historico) < self.janela:
            return Decisao("AGUARDANDO", None)

        media = sum(historico) / len(historico)
        if preco > media * (1 + self.limiar):
            return Decisao("VENDER", media)
        if preco < media * (1 - self.limiar):
            return Decisao("COMPRAR", media)
        return Decisao("MANTER", media)

"""
infra/terminal.py - cores e formatação para os logs no console.

Não tem nada a ver com Kafka: só deixa a saída dos serviços mais fácil de
ler durante a demonstração (cada partição ganha uma cor, decisões coloridas,
valores no formato brasileiro).
"""
import os

if os.name == "nt":
    # Truque conhecido no Windows: executar um comando vazio ativa o suporte
    # a cores ANSI no console clássico (no Windows Terminal/VS Code já funciona).
    os.system("")


class Cor:
    FIM = "\033[0m"
    NEGRITO = "\033[1m"
    CINZA = "\033[90m"
    VERMELHO = "\033[91m"
    VERDE = "\033[92m"
    AMARELO = "\033[93m"
    AZUL = "\033[94m"
    MAGENTA = "\033[95m"
    CIANO = "\033[96m"


# Uma cor por partição: no vídeo fica fácil ver qual instância lê qual partição.
_CORES_PARTICAO = [Cor.CIANO, Cor.MAGENTA, Cor.AZUL, Cor.AMARELO]


def colorir(texto: str, cor: str) -> str:
    return f"{cor}{texto}{Cor.FIM}"


def rotulo_particao(particao: int) -> str:
    """Ex.: 'p0' em ciano, 'p1' em magenta..."""
    return colorir(f"p{particao}", _CORES_PARTICAO[particao % len(_CORES_PARTICAO)])


def numero(valor: int) -> str:
    """1234567 -> '1.234.567'"""
    return f"{valor:,}".replace(",", ".")


def brl(valor: float) -> str:
    """1234.5 -> 'R$ 1.234,50'"""
    texto = f"{valor:,.2f}".replace(",", "#").replace(".", ",").replace("#", ".")
    return f"R$ {texto}"


def segundos(valor: float) -> str:
    """0.25 -> '0,3 s'"""
    return f"{valor:.1f} s".replace(".", ",")

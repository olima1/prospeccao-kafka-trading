"""
Testes das regras do cenário. Rodam SEM Kafka e sem Docker - um efeito
colateral bom do desacoplamento: a lógica não depende da infraestrutura.

    python -m unittest discover -s testes -v
"""
import unittest

from dominio.estrategia import EstrategiaMediaMovel
from dominio.mercado import ATIVOS_PADRAO, MercadoSimulado


class TestEstrategiaMediaMovel(unittest.TestCase):
    def setUp(self):
        self.estrategia = EstrategiaMediaMovel(janela=5, limiar=0.002)

    def alimentar(self, ativo, precos):
        decisao = None
        for preco in precos:
            decisao = self.estrategia.avaliar(ativo, preco)
        return decisao

    def test_aguarda_ate_completar_a_janela(self):
        for preco in [10.0, 10.0, 10.0, 10.0]:
            self.assertEqual(self.estrategia.avaliar("PETR4", preco).acao, "AGUARDANDO")
        self.assertEqual(self.estrategia.avaliar("PETR4", 10.0).acao, "MANTER")

    def test_preco_acima_da_media_vende(self):
        self.assertEqual(self.alimentar("PETR4", [10.0] * 4 + [10.5]).acao, "VENDER")

    def test_preco_abaixo_da_media_compra(self):
        self.assertEqual(self.alimentar("PETR4", [10.0] * 4 + [9.5]).acao, "COMPRAR")

    def test_historico_e_separado_por_ativo(self):
        self.alimentar("PETR4", [10.0] * 5)
        self.assertEqual(self.estrategia.avaliar("VALE3", 60.0).acao, "AGUARDANDO")


class TestMercadoSimulado(unittest.TestCase):
    def test_evento_v1_segue_o_contrato(self):
        evento = MercadoSimulado(["PETR4"], semente=1).gerar_evento("PETR4", versao=1)
        self.assertEqual(set(evento), {"id_evento", "versao", "ativo", "preco", "emitido_em"})
        self.assertEqual(evento["versao"], 1)
        self.assertGreater(evento["preco"], 0)

    def test_evento_v2_so_acrescenta_volume(self):
        evento = MercadoSimulado(["PETR4"], semente=1).gerar_evento("PETR4", versao=2)
        self.assertEqual(evento["versao"], 2)
        self.assertIn("volume", evento)

    def test_mesma_semente_gera_os_mesmos_precos(self):
        a, b = MercadoSimulado(ATIVOS_PADRAO, semente=42), MercadoSimulado(ATIVOS_PADRAO, semente=42)
        self.assertEqual([a.proximo_preco("VALE3") for _ in range(10)],
                         [b.proximo_preco("VALE3") for _ in range(10)])


if __name__ == "__main__":
    unittest.main()

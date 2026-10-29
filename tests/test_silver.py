from datetime import date, datetime

from conftest import comex, sidra, usina
from siderurgia_lakehouse.transform import silver


# ----------------------------------------------------------------------------- produção
def test_producao_tipa_e_mapeia_metrica(bronze):
    df, quarentena = silver.construir_producao(
        bronze(
            "sidra_pimpf", [sidra("202401", "98.12345"), sidra("202401", "97.5", variavel="12607")]
        )
    )
    linhas = {r.metrica: r for r in df.collect()}
    assert set(linhas) == {"indice", "indice_sa"}
    assert linhas["indice"].mes_referencia == date(2024, 1, 1)
    assert float(linhas["indice"].valor) == 98.12345
    assert linhas["indice"].atividade_nome == "3.24 Metalurgia"
    assert quarentena.count() == 0


def test_producao_simbolos_do_ibge_e_layout_vao_para_quarentena(bronze):
    df, quarentena = silver.construir_producao(
        bronze(
            "sidra_pimpf",
            [
                sidra("202401", "..."),  # valor não disponível
                sidra("202401", "X", atividade=("129335", "3.25 Produtos de metal")),  # sigilo
                sidra("2024-01", "98"),  # período fora do padrão
                sidra("202401", "98", variavel="9999"),  # variável inesperada
            ],
        )
    )
    assert df.count() == 0
    assert sorted(r.motivo_rejeicao for r in quarentena.collect()) == [
        "layout_inesperado", "periodo_invalido", "valor_indisponivel", "valor_indisponivel",
    ]  # fmt: skip


def test_producao_mantem_revisao_mais_recente(bronze):
    antigo = bronze(
        "sidra_pimpf", [sidra("202401", "98")], lote="L1", ingerido=datetime(2024, 3, 1)
    )
    revisto = bronze(
        "sidra_pimpf", [sidra("202401", "99")], lote="L2", ingerido=datetime(2024, 4, 1)
    )
    df, _ = silver.construir_producao(antigo.unionByName(revisto))
    assert [float(r.valor) for r in df.collect()] == [99.0]


# ----------------------------------------------------------------------------- exportações
def test_exportacoes_parse_basico(bronze):
    df, _ = silver.construir_exportacoes(bronze("comex", [comex(2024, 3, "China", 1500.5, 2000)]))
    r = df.first()
    assert r.mes_referencia == date(2024, 3, 1)
    assert r.pais == "China" and r.sh4_codigo == "7208"
    assert float(r.valor_fob_usd) == 1500.5 and float(r.peso_kg) == 2000


def test_exportacoes_aceita_nomes_alternativos_de_campos(bronze):
    # Mesmo registro com nomes antigos/alternativos e o código SH4 só dentro da descrição
    alternativo = {
        "coAno": "2024", "coMes": "03", "noPais": "Argentina",
        "noSh4Por": "7213 - Fio-máquina de ferro ou aço", "vlFob": "10", "kgLiquido": "5",
    }  # fmt: skip
    df, quarentena = silver.construir_exportacoes(bronze("comex", [alternativo]))
    r = df.first()
    assert (r.pais, r.sh4_codigo, r.mes_referencia) == ("Argentina", "7213", date(2024, 3, 1))
    assert quarentena.count() == 0


def test_exportacoes_layout_desconhecido_vai_para_quarentena(bronze):
    df, quarentena = silver.construir_exportacoes(bronze("comex", [{"campo_novo": "x"}]))
    assert df.count() == 0
    assert quarentena.first().motivo_rejeicao == "layout_inesperado"


# ----------------------------------------------------------------------------- emissões
def test_emissoes_parse_e_ano_da_requisicao(bronze):
    sem_ano = usina("u1", "Usina A", 1234.5)
    del sem_ano["year"]
    df, _ = silver.construir_emissoes(bronze("ct", [sem_ano], requisicao={"year": 2022}))
    r = df.first()
    assert (r.usina_id, r.ano, float(r.emissoes_t)) == ("u1", 2022, 1234.5)
    assert float(r.latitude) == -20.1


def test_emissoes_fora_do_escopo_vao_para_quarentena(bronze):
    df, quarentena = silver.construir_emissoes(
        bronze(
            "ct",
            [
                usina("u1", "Brasil aço", 10),
                usina("u2", "Outro país", 10, pais="CHN"),
                usina("u3", "Outro setor", 10, subsetor="cement"),
            ],
        )
    )
    assert [r.usina_id for r in df.collect()] == ["u1"]
    assert {r.motivo_rejeicao for r in quarentena.collect()} == {"fora_do_escopo"}


# ----------------------------------------------------------------------------- câmbio
def test_cambio_descarta_data_futura(bronze):
    df, quarentena = silver.construir_cambio(
        bronze(
            "bcb", [{"data": "02/01/2024", "valor": "4.89"}, {"data": "01/01/2099", "valor": "1"}]
        ),
        hoje=date(2024, 6, 1),
    )
    assert df.count() == 1
    assert quarentena.first().motivo_rejeicao == "data_futura"

-- Consultas de exemplo sobre a camada gold (Databricks SQL / notebook).
-- Localmente, troque `siderurgia_lakehouse.gold.<tabela>` por delta.`lakehouse/gold/<tabela>`.

-- 1) Painel dos últimos 24 meses fechados: produção da metalurgia x exportações x câmbio
SELECT mes_referencia,
       indice_metalurgia_sa,
       metalurgia_var_12m_pct,
       exportacao_t,
       preco_medio_usd_t,
       dolar_medio
FROM siderurgia_lakehouse.gold.painel_siderurgia_mensal
WHERE mes_fechado
ORDER BY mes_referencia DESC
LIMIT 24;

-- 2) O câmbio puxa as exportações? Correlação entre dólar e toneladas exportadas, por ano
SELECT ano,
       ROUND(CORR(dolar_medio, exportacao_t), 3)       AS corr_dolar_volume,
       ROUND(CORR(dolar_medio, preco_medio_usd_t), 3)  AS corr_dolar_preco
FROM siderurgia_lakehouse.gold.painel_siderurgia_mensal
WHERE mes_fechado
GROUP BY ano
ORDER BY ano;

-- 3) A metalurgia acompanha a indústria? Diferença entre os índices ao longo do tempo
SELECT ano,
       ROUND(AVG(indice_metalurgia), 1)     AS metalurgia,
       ROUND(AVG(indice_transformacao), 1)  AS transformacao,
       ROUND(AVG(indice_metalurgia) - AVG(indice_transformacao), 1) AS diferenca
FROM siderurgia_lakehouse.gold.painel_siderurgia_mensal
GROUP BY ano
ORDER BY ano;

-- 4) Principais destinos do aço brasileiro nos últimos 12 meses fechados
SELECT pais,
       ROUND(SUM(peso_t))                                  AS toneladas,
       ROUND(SUM(valor_fob_usd) / 1e6, 1)                  AS fob_usd_milhoes,
       ROUND(SUM(valor_fob_usd) / SUM(peso_t), 0)          AS preco_medio_usd_t,
       ROUND(100 * SUM(peso_t) / SUM(SUM(peso_t)) OVER (), 1) AS participacao_pct
FROM siderurgia_lakehouse.gold.fato_exportacao_aco
WHERE mes_referencia >= add_months(trunc(current_date(), 'MM'), -12)
  AND mes_referencia <  trunc(current_date(), 'MM')
GROUP BY pais
ORDER BY toneladas DESC
LIMIT 15;

-- 5) Mix de produtos exportados (posição SH4) por ano
SELECT year(mes_referencia) AS ano, sh4_codigo, ANY_VALUE(sh4_descricao) AS produto,
       ROUND(SUM(peso_t)) AS toneladas
FROM siderurgia_lakehouse.gold.fato_exportacao_aco
GROUP BY ALL
ORDER BY ano DESC, toneladas DESC;

-- 6) Emissões: as 10 usinas que mais emitiram no último ano disponível
SELECT ranking_no_ano, usina_nome, emissoes_t_co2e, participacao_pct, latitude, longitude
FROM siderurgia_lakehouse.gold.fato_emissoes_usina
WHERE ano = (SELECT MAX(ano) FROM siderurgia_lakehouse.gold.fato_emissoes_usina)
ORDER BY ranking_no_ano
LIMIT 10;

-- 7) Concentração: quanto das emissões do setor vem das 3 maiores usinas, por ano
SELECT ano, ROUND(SUM(participacao_pct), 1) AS top3_pct
FROM siderurgia_lakehouse.gold.fato_emissoes_usina
WHERE ranking_no_ano <= 3
GROUP BY ano
ORDER BY ano;

-- 8) Delta Lake: auditoria e Time Travel
DESCRIBE HISTORY siderurgia_lakehouse.silver.exportacoes_aco;

SELECT COUNT(*) FROM siderurgia_lakehouse.silver.exportacoes_aco VERSION AS OF 0;

-- 9) Qualidade: o que caiu na quarentena, por fonte e motivo
SELECT _fonte, motivo_rejeicao, COUNT(*) AS qtd, MAX(_criado_em) AS ultima_ocorrencia
FROM siderurgia_lakehouse.silver.quarentena
GROUP BY ALL
ORDER BY qtd DESC;

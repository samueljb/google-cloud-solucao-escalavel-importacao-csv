# View criada no Bigquery, com os indicadores mais relevantes
# Nome: seu-projeto-nome-no-google:dataNavigationDataSet.SalesKPI

    SELECT  'Abandono de Carrinho de compras' as Description, basket.losing as q1, thankyou.buy as q2, 100-ROUND((thankyou.buy / basket.losing)*100, 2) as rate  FROM
(SELECT count(distinct visit_id) as losing FROM `seu-projeto-nome-no-google.dataNavigationDataSet.RAW_DATA_NAVIGATION` 
where page_type = 'basket') basket
CROSS JOIN
(SELECT count(distinct visit_id) as buy FROM `seu-projeto-nome-no-google.dataNavigationDataSet.RAW_DATA_NAVIGATION` 
where page_type = 'thankyou') thankyou
union all
SELECT  'Taxa de conversão vindo da home', home.visit, thankyou.buy, ROUND((thankyou.buy / home.visit)*100, 2) as conversion_rate_home  FROM
(SELECT count(distinct visit_id) as visit FROM `seu-projeto-nome-no-google.dataNavigationDataSet.RAW_DATA_NAVIGATION` 
where page_type = 'home') home
CROSS JOIN
(SELECT count(distinct visit_id) as buy FROM `seu-projeto-nome-no-google.dataNavigationDataSet.RAW_DATA_NAVIGATION` 
where page_type = 'thankyou') thankyou
union all
SELECT  'Taxa de conversão vindo da Busca', home.visit, thankyou.buy, ROUND((thankyou.buy / home.visit)*100, 2) as conversion_rate_home  FROM
(SELECT count(distinct visit_id) as visit FROM `seu-projeto-nome-no-google.dataNavigationDataSet.RAW_DATA_NAVIGATION` 
where page_type = 'search') home
CROSS JOIN
(SELECT count(distinct visit_id) as buy FROM `seu-projeto-nome-no-google.dataNavigationDataSet.RAW_DATA_NAVIGATION` 
where page_type = 'thankyou') thankyou
union all
SELECT  'Taxa de conversão vindo do Produto', home.visit, thankyou.buy, ROUND((thankyou.buy / home.visit)*100, 2) as conversion_rate_home  FROM
(SELECT count(distinct visit_id) as visit FROM `seu-projeto-nome-no-google.dataNavigationDataSet.RAW_DATA_NAVIGATION` 
where page_type = 'product') home
CROSS JOIN
(SELECT count(distinct visit_id) as buy FROM `seu-projeto-nome-no-google.dataNavigationDataSet.RAW_DATA_NAVIGATION` 
where page_type = 'thankyou') thankyou
union all
SELECT  'Não pagamento - Desistencia', home.visit, thankyou.buy, 100-ROUND((thankyou.buy / home.visit)*100, 2) as conversion_rate_home  FROM
(SELECT count(distinct visit_id) as visit FROM `seu-projeto-nome-no-google.dataNavigationDataSet.RAW_DATA_NAVIGATION` 
where page_type = 'payment') home
CROSS JOIN
(SELECT count(distinct visit_id) as buy FROM `seu-projeto-nome-no-google.dataNavigationDataSet.RAW_DATA_NAVIGATION` 
where page_type = 'thankyou') thankyou


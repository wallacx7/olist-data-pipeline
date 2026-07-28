-- Teste customizado: valida que nenhum pedido tem tempo de entrega negativo
-- (o que indicaria erro de dados: entrega registrada antes da compra).
-- Convenção dbt: um teste "falha" se a query retornar QUALQUER linha.
-- Portanto, aqui selecionamos os casos que NÃO deveriam existir.

select
    order_id,
    delivery_days
from {{ ref('stg_orders') }}
where delivery_days < 0
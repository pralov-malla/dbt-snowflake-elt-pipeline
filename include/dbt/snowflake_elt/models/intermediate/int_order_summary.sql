select
    order_id,
    count(*) as total_items,
    sum(price) as total_item_price,
    sum(freight_value) as total_freight,
    sum(item_total_cost) as total_order_value
from {{ ref('int_order_items') }}
group by order_id

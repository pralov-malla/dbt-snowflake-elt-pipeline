select
    items.order_id,
    items.order_item_id,
    items.product_id,
    items.seller_id,
    items.price,
    items.freight_value,
    orders.customer_id,
    orders.order_status,
    orders.order_purchase_timestamp,
    -- custom macro in action
    {{ total_item_cost('items.price', 'items.freight_value') }} as item_total_cost
from {{ ref('stg_olist__orders') }} as orders
inner join {{ ref('stg_olist__order_items') }} as items
    on orders.order_id = items.order_id

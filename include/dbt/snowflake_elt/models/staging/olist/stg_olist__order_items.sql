select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    -- Cast string timestamps to proper timestamp objects
    try_cast(shipping_limit_date as timestamp) as shipping_limit_date,
    -- Ensure numeric types
    cast(price as number(10,2)) as price,
    cast(freight_value as number(10,2)) as freight_value
from {{ source('olist', 'order_items') }}

select
    orders.order_id,
    orders.customer_id,
    orders.order_status,
    orders.order_purchase_timestamp,
    orders.order_approved_at,
    orders.order_delivered_carrier_date,
    orders.order_delivered_customer_date,
    orders.order_estimated_delivery_date,
    order_summary.total_items,
    order_summary.total_item_price,
    order_summary.total_freight,
    order_summary.total_order_value,
    payment_summary.total_payment_value,
    payment_summary.distinct_payment_types
from {{ ref('stg_olist__orders') }} as orders
inner join {{ ref('int_order_summary') }} as order_summary
    on orders.order_id = order_summary.order_id
left join (
    select
        order_id,
        sum(payment_value) as total_payment_value,
        count(distinct payment_type) as distinct_payment_types
    from {{ ref('stg_olist__payments') }}
    group by order_id
) as payment_summary
    on orders.order_id = payment_summary.order_id

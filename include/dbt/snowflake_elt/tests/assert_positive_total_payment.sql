-- Singular test: Every order that has payments should have a positive total.
-- If this query returns any rows, the test FAILS.
select
    order_id,
    total_payment_value
from {{ ref('fct_orders') }}
where total_payment_value is not null
  and total_payment_value <= 0

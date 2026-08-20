{% macro total_item_cost(column_price, column_freight) %}
    ({{ column_price }} + {{ column_freight }})
{% endmacro %}

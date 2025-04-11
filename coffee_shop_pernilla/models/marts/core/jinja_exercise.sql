{# select
  date_trunc(sold_at, month) as date_month,
  sum(case when product_category = 'coffee beans' then amount end) as coffee_beans_amount,
  sum(case when product_category = 'merch' then amount end) as merch_amount,
  sum(case when product_category = 'brewing supplies' then amount end) as brewing_supplies_amount
from {{ ref('item_sales') }}
group by 1 #}
{%- set product_category = dbt_utils.get_column_values(
    table=ref('item_sales'),
    column='product_category'
) -%}
{# {%- set product_category=get_product_categories()-%} #}
select
  date_trunc(sold_at, month) as date_month,
  {%- for prod_categ in product_category %}
  sum(case when product_category = '{{prod_categ}}' then amount end) as {{prod_categ | replace(" ","_")}}_amount
  {%- if not loop.last %},{% endif -%}
  {% endfor %}
from {{ ref('item_sales') }}
group by 1
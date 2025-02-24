My macro


{% macro get_order_linenos() %}
 
{% set order_linenos_query %}
 
select distinct
lineno
from {{ ref('fct_orders') }}
order by 1
 
{% endset %}
 
{% set results = run_query(order_linenos_query) %}
 
{% if execute %}
 
{# Return the first column #}
 
{% set results_list = results.columns[0].values() %}
 
{% else %}
 
{% set results_list = [] %}
 
{% endif %}
 
 
{% endmacro %}


{####################################################}
{####################################################}
{# Min Order date calculation #}
{####################################################}
{####################################################}
{% macro get_min_order_date() %}
 
{% set min_order_date_query %}
 
select 
min(ORDERDATE)
from {{ ref('trf_orders') }}
 
{% endset %}
 
{% set results = run_query(min_order_date_query)%}
 
{% if execute %}
 
{# Return the first column #}
 
{% set results_list = results.columns[0][0] %}
 
{% else %}
 
{% set results_list = [] %}
 
{% endif %}
 
{{ return(results_list) }}

{% endmacro %}



{####################################################}
{####################################################}
{# Max Order date calculation #}
{####################################################}
{####################################################}

{% macro get_max_order_date() %}
 
{% set max_order_date_query %}
 
select 
max(ORDERDATE)
from {{ ref('trf_orders') }}
 
{% endset %}
 
{% set results = run_query(max_order_date_query) %}
 
{% if execute %}
 
{# Return the first column #}
 
{% set results_list = results.columns[0][0] %}
 
{% else %}
 
{% set results_list = [] %}
 
{% endif %}
 
{{ return(results_list) }}

{% endmacro %}


{{config(materialized = 'view',schema = 'Reporting_dev')}}


--- below is a normal query
/*

SELECT
ORDERID,
sum(case when lineno=1 then line_sale_amount else 0 end) as LINENO1_SALES
,sum(case when lineno=2 then line_sale_amount else 0 end) as LINENO2_SALES
,sum(case when lineno=3 then line_sale_amount else 0 end) as LINENO3_SALES
,sum(line_sale_amount) as TOTAL_SALES
FROM {{ref('fct_orders')}}
group by ORDERID

*/
--the above query can be simplified by using zinga templates and for loop.


SELECT
ORDERID,

{% for lineno in [1,2,3,4] %} 

sum(case when lineno= {{ lineno }} then line_sale_amount else 0 end) as LINENO{{ lineno }}_SALES,

{% endfor %}

sum(line_sale_amount) as TOTAL_SALES

FROM {{ref('fct_orders')}}

group by ORDERID


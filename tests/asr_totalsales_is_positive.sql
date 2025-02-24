select orderid, sum(LINE_SALE_AMOUNT) as sales
from {{ref('fct_orders')}}
group by orderid
having sales<0

select orderid, count(LINENO ) as totallineno
from {{ref('fct_orders')}}
group by orderid
having totallineno < 1
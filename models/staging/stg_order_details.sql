{{config(materialized='incremental', transient=false, unique_key = ['orderid','lineno'])}}

with raw_orders as (select * from
{{source('raw_qwt','raw_orders')}})
,
order_details as (
select * from
{{source('raw_qwt','raw_order_details')}} 
)

select 
od.OrderID 
, o.orderdate
, od.LineNo 
, od.ProductID 
, od.Quantity 
, od.UnitPrice 
, od.Discount
from order_details as od
join raw_orders as o on o.orderid=od.OrderID

{% if is_incremental() %}

where o.orderdate > (select max(orderdate) from {{this}})

{% endif %}
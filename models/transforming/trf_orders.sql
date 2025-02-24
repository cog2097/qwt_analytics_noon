{{config(materialized='table',schema=env_var('DBT_TRANSFORMSCHEMA','transforming_dev') )}}

select
o.ORDERID
, od.lineno
, o.CUSTOMERID
, o.EMPLOYEEID
, o.SHIPPERID
, od.productid
, od.quantity
, o.FREIGHT
, od.unitprice
, od.discount
, o.orderdate
, to_decimal((od.unitprice * od.quantity) * (1-od.Discount),9,2) as Line_sale_amount
, to_decimal((p.unit_cost * od.quantity),9,2) as Cost_of_Good_sold
, to_decimal(((od.unitprice * od.quantity) * (1-od.Discount)) - (p.unit_cost * od.quantity),9,2) as margin
from
{{ref('stg_orders')}} as o
inner join {{ref('stg_order_details')}} as od on o.orderid=od.orderid
inner join {{ref('stg_products')}} as p on p.product_id = od.productid
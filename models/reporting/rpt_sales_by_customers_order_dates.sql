{{config(materialized = 'view',schema = 'Reporting_dev')}}

select 
c.COMPANYNAME
, c.contact_name
, min(o.orderdate) as first_order_date
--, iff(o.orderdate=d.DATE_DAY,d.DAY_OF_WEEK,'not found') as first_order_day
, max(o.orderdate) as recent_order_date
, count(o.orderid) as total_orders
, count(distinct p.product_id) as no_of_products
, sum(o.quantity) as total_quantity
, sum(o.line_sale_amount) as total_Sales
, avg(o.margin) as avg_amrgin
from {{ref('fct_orders')}} as o
inner join {{ref('dim_customers')}} as c
    on o.customerid=c.customer_id
inner join {{ref('dim_products')}} p
    on p.product_id = o.productid
inner join {{ref('dim_date')}} as d
    on d.date_day= o.orderdate
group by c.companyname, c.contact_name
order by total_sales desc


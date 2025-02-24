{{config(materialized='view',schema='reporting_dev')}}

select
e.country
, c.companyname
, c.contact_name
, count(o.orderid) as total_orders
, sum(o.quantity) as total_quantity
, sum(o.line_sale_amount) as total_sales
, avg(o.margin) as avg_margin
from
{{ref('fct_orders')}} as o
inner join {{ref('dim_customers')}} as c
    on o.customerid=c.customer_id
inner join {{ref("dim_employee")}} as e
    on o.employeeid=e.EMPLOYEE_ID
where e.country='{{var('v_country','Sweden')}}'
group by e.country, c.companyname, c.contact_name
order by total_sales desc
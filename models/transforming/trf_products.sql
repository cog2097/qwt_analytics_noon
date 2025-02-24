{{config(materialized='table',schema=env_var('DBT_TRANSFORMSCHEMA','transforming_dev'))}}

select 
p.PRODUCT_ID
, p.PRODUCT_NAME
, c.categoryname
, s.city as suppliercity
, s.country as suplliercountry
, p.quantity_per_unit
, p.unit_cost
, p.unit_price
, p.UNITS_IN_STORE
, p.UNITS_ON_ORDER
, p.unit_price - p.unit_cost as profit
, iff(p.UNITS_IN_STORE > p.UNITS_ON_ORDER, 'Available', 'Not Available') as Productavailability
from {{ref('stg_products')}} as p
inner join {{ref('trf_suppliers')}} as s 
    on p.SUPLIER_ID=s.SupplierID
inner join {{ref('lkp_categories')}} as c
    on p.CATEGORY_ID = c.categoryid
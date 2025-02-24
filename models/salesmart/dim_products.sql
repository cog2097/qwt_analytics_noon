{{config(materialized='view',schema=env_var('DBT_SALESMARTSCHEMA','salesmart_dev'))}}

select *
from {{ref("trf_products")}}
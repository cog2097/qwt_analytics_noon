{{config(materialized='table',schema=env_var('DBT_TRANSFORMSCHEMA','transforming_dev'))}}

select 
get(xmlget(SUPPLIERINFO,'SupplierID'),'$') as SupplierID
, get(xmlget(SUPPLIERINFO,'CompanyName'),'$')::varchar as CompanyName
, get(xmlget(SUPPLIERINFO,'ContactName'),'$')::varchar as ContactName
, get(xmlget(SUPPLIERINFO,'Address'),'$')::varchar as Address
, get(xmlget(SUPPLIERINFO,'City'),'$')::varchar as City
, get(xmlget(SUPPLIERINFO,'PostalCode'),'$')::varchar as PostalCode
, get(xmlget(SUPPLIERINFO,'Country'),'$')::varchar as Country
, get(xmlget(SUPPLIERINFO,'Phone'),'$')::varchar as Phone
, get(xmlget(SUPPLIERINFO,'Fax'),'$')::varchar as Fax
from {{ref('stg_suppliers')}}
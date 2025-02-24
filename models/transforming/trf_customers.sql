{{config(materialized='table',schema= env_var('DBT_TRANSFORMSCHEMA','transforming_dev'))}}

select 
c.CUSTOMER_ID
, c.COMPANYNAME 
, c.CONTACT_NAME 
, c.CITY 
, c.COUNTRY 
, d.divisionname
, c.ADDRESS 
, c.FAX 
, c.PHONE 
, c.POSTAL_CODE 
, iff(c.STATE_PROVINCE ='','NA',c.STATE_PROVINCE) as STATE_PROVINCE_NAME
from
{{ref('stg_customers')}} as c
inner join {{ref('lkp_divisions')}} d on c.DIVISION_ID = d.divisionid
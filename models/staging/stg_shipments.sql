{{config(materialized='table',transient=false)}}

select
orderid 
, LINENO 
, SHIPPERID 
, CUSTOMERID 
, PRODUCTID 
, EMPLOYEEID 
, split_part(SHIPMENTDATE,' ',1)::date as SHIPMENTDATE 
, STATUS 
from {{source('raw_qwt','raw_shipments')}}
{{config(materialized='table',transient=false)}}

select 
office as ID
 , OfficeAddress as ADDRESS
 , OfficePostalCode AS POSTALCODE
 , OfficeCity AS CITY
 , OfficeStateProvince AS STATEPROVINCE
 , OfficePhone AS PHONE
 , OfficeFax AS FAX
 , OfficeCountry AS COUNTRY
 from
{{source('raw_qwt','raw_offices')}}
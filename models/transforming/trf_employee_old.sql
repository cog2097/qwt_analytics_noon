{{config(materialized='table',schema=env_var('DBT_TRANSFORMSCHEMA','transforming_dev'))}}

select
emp.EmpID
, emp.LastName
, emp.FirstName
, emp.Title
, emp.HireDate
, emp.Extension
, IFF(mgr.FirstName is null or mgr.lastname is null, emp.FirstName||' '|| emp.lastname, mgr.FirstName||' '|| mgr.lastname) AS MANAGER_NAME
, IFF(mgr.title is null,Emp.title,mgr.Title) AS MANAGER_title
, emp.YearSalary
, ofc.ADDRESS
, ofc.CITY
, ofc.COUNTRY
from {{ref('stg_employee')}} emp
left join {{ref('stg_employee')}} mgr 
    on emp.ReportsTo = mgr.empid
left join {{ref('stg_offices')}} ofc 
    on ofc.id=emp.office
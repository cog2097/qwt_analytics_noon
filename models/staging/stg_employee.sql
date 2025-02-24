{{config(materialized='table',transient=false)}}

select 
EmpID
, LastName
, FirstName
, Title
, HireDate
, Office
, IFF(Extension='-','NA',Extension) AS Extension
, ReportsTo
, YearSalary
from
{{source('raw_qwt','raw_employee')}}
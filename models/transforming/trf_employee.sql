{{config(materialized='table',schema=env_var('DBT_TRANSFORMSCHEMA','transforming_dev'))}}

with recursive managers 
      -- Column names for the "view"/CTE
      (indent,office_id, employee_id,employee_name, manager_id, employee_title, manager_title,manager_name) 
    as
      -- Common Table Expression
      (
 
        -- Anchor Clause
        select '*' as indent, office as office_id, EMPID as employee_id, firstname||' '||lastname as employee_name,  reportsto AS manager_id, title as employee_title,  title as manager_title,firstname as manager_name
          from {{ref('stg_employee')}}
          where title = 'President'
 
 
        union all
 
 
        -- Recursive Clause
        select indent || '* ',employees.office as office_id,
            employees.EMPID,employees.firstname||' '||employees.lastname as employee_name, employees.reportsto as manager_id, employees.title, managers.employee_title, managers.employee_name as manager_name
          from {{ref('stg_employee')}} as employees join managers  
            on employees.reportsto = managers.employee_id
      ),
 
      offices(office_id, office_city, office_country)
      as
      (
      select id, city, country from {{ref('stg_offices')}}
      )
 
  select indent, employee_id, employee_name, employee_title , manager_id, manager_name, manager_title, ofc.office_city, ofc.office_country
    from managers as mgr inner join offices as ofc on ofc.office_id = mgr.office_id
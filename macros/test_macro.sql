{% test is_even(model, column_name) %}
 
with validation as (
 
    select
        {{ column_name }} as even_field
 
    from {{ model }}
 
),
 
validation_errors as (
 
    select
        even_field
 
    from validation
    -- if this is true, then even_field is actually odd!
    where (even_field % 2) = 1
 
)
 
select *
from validation_errors
 
{% endtest %}


{% test is_odd(model, column_name) %}
 
with validation as (
 
    select
        {{ column_name }} as odd_field
 
    from {{ model }}
 
),
 
validation_errors as (
 
    select
        odd_field
 
    from validation
    -- if this is true, then even_field is actually odd!
    where (odd_field % 2) <> 1
 
)
 
select *
from validation_errors
 
{% endtest %}
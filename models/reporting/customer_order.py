import snowflake.snowpark.functions as F
 
 
def model(dbt, session):
 
    dbt.config(
        materialized = "table", schema = "reporting_dev",
    )
 
    dim_customers_df = dbt.ref('dim_customers')
    fct_orders_df = dbt.ref('fct_orders')
 
    final_df = (
       dim_customers_df
       .join(fct_orders_df, dim_customers_df.customer_id == fct_orders_df.customerid, 'left')
       .select(dim_customers_df.customer_id,
               dim_customers_df.companyname,
               dim_customers_df.contact_name,
               fct_orders_df.orderdate,
               fct_orders_df.orderid,
               fct_orders_df.LINE_SALE_AMOUNT,
               fct_orders_df.quantity,
               fct_orders_df.margin
 
               
       )
   )
 
 
   
    return final_df
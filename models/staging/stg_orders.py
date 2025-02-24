import snowflake.snowpark.functions as F
import pandas as pd

def model(dbt,session):

    dbt.config(materialized='incremental',unique_key=['orderid'])

    order_df= dbt.source("raw_qwt",'raw_orders')

    if dbt.is_incremental:
        max_order_date= F"select max(ORDERDATE) from {dbt.this}"
        
        order_df=order_df.filter(order_df.orderdate > session.sql(max_order_date).collect()[0][0])

    return order_df
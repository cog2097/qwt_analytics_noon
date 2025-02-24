import snowflake.snowpark.functions as F
import pandas as pd
import holidays

def is_holiday(holiday_date):
    french_holidays = holidays.France()

    is_holiday = (holiday_date in french_holidays)

    return is_holiday

def avgordervalue(nooforders,totalvalue):

    return totalvalue/nooforders
 
def model(dbt, session):
 
    dbt.config(
        materialized = "table", schema = "reporting_dev", packages = ['holidays'], pre_hook = 'use warehouse python_models_wh;'
    )

    orders_df = dbt.ref('fct_orders')

    orders_agg_df = (
                    orders_df
                    .group_by('CUSTOMERID')
                    .agg
                    (
                         F.min(F.col('orderdate')).alias('first_order_date')
                        , F.max(F.col('orderdate')).alias('recent_order_date')
                        , F.count(F.col('orderid')).alias('total_order')
                        , F.countDistinct(F.col('PRODUCTID')).alias('NoofProducts')
                        , F.sum(F.col('quantity')).alias('total_quantity')
                        , F.sum(F.col('line_sale_amount')).alias('total_Sales')
                        , F.avg(F.col('margin')).alias('avg_margin')
                    )
    )
    
    customer_df = dbt.ref('dim_customers')

    customer_orders_df = (
                        customer_df
                        .join(orders_agg_df,orders_agg_df.CUSTOMERID == customer_df.customer_id, 'left')
                        .select(customer_df.companyname
                                ,   customer_df.contact_name
                                ,   orders_agg_df.first_order_date
                                ,   orders_agg_df.recent_order_date
                                ,   orders_agg_df.total_order
                                ,   orders_agg_df.NoofProducts
                                ,   orders_agg_df.total_quantity
                                ,   orders_agg_df.total_Sales
                                ,   orders_agg_df.avg_margin

                        )
    )
    
    customer_orders_final_df = customer_orders_df.withColumn('averageorderamount',avgordervalue(customer_orders_df["total_quantity"],customer_orders_df["total_quantity"]))

    final_df = customer_orders_df.filter(F.col('first_order_date').isNotNull())

    final_order_df = final_df.to_pandas()
    final_order_df["is_first_order_on_holiday"] = final_order_df["FIRST_ORDER_DATE"].apply(is_holiday)

    return final_order_df


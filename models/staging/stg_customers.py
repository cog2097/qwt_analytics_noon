
def model(dbt, session):

        customers_df=dbt.source("raw_qwt","customer_raw")

        return customers_df
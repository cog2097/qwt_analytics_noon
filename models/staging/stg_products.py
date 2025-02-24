
def model(dbt, session):

        products_df=dbt.source("raw_qwt","raw_products")

        return products_df
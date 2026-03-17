import pyspark.sql.types as T 

retail_sales_bronze = (T.StructType([
    T.StructField("transaction_id", T.IntegerType(), True),
    T.StructField("order_date", T.DateType(), True),
    T.StructField("ship_date", T.DateType(), True),
    T.StructField("customer_id", T.StringType(), True),
    T.StructField("customer_age", T.IntegerType(), True),
    T.StructField("gender", T.StringType(), True),
    T.StructField("product_id", T.StringType(), True),
    T.StructField("product_category", T.StringType(), True),
    T.StructField("quantity", T.IntegerType(), True),
    T.StructField("unit_price", T.DoubleType(), True),
    T.StructField("discount_pct", T.DoubleType(), True),
    T.StructField("city", T.StringType(), True),
    T.StructField("state", T.StringType(), True),
    T.StructField("payment_type", T.StringType(), True),
    T.StructField("order_status", T.StringType(), True),
    T.StructField("ingestion_date", T.TimestampType(), True),
]))
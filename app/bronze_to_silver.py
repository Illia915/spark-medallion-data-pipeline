import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.sql import SparkSession 
from pathlib import Path 

#Випиши в окремий файл 
spark = (
    SparkSession.builder\
        .appName("BronzeToSilver")\
        .getOrCreate()
)

# Винести дані в utils 
SPARK_APP = Path('/opt/spark-app')
SPARK_DATA = Path('/opt/spark-data')
bronze_file_path = Path(SPARK_DATA / f"bronze/*.parquet")


bronze_df = (
    spark.read\
        .parquet(str(bronze_file_path))
)

bronze_df.printSchema()


"""
1. Check for dublicated transection id 
2. Chek for record where ship_date < order_date 
3. Maybe clean customer_id CUST*** -> *** and if dome record start not from CUST 
4. Check for diffrents gender records .groupBy(gender, count())
5. CHECK IF ANY RECORD STARTS NOT WITH PROD 
6. Check for diff types of product_category
7. Check if quantity parametr is > 0 
8. Check for unvalid unit_price
9. Check if discount_pct < 100 anf for invalid records 
10. Also dicide what to fo with null values 
"""
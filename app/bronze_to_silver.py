from pathlib import Path 

import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, Window

import utils.config as cfg

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
silver_file_path = Path(SPARK_DATA / f"silver")


bronze_df = (
    spark.read\
        .parquet(str(bronze_file_path))
)

def drop_duplicates_by_smallest_date(df):
    
    window_spec = Window.partitionBy("transaction_id").orderBy("order_date")
    
    silver_df = (
        df.withColumn("row_num", F.row_number().over(window_spec))\
        .filter(F.col("row_num") == 1)\
        .drop("row_num")
    )      
    
    return silver_df

def correct_shiping_date(df):
    
    silver_df = df.withColumn(
            "ship_date", F.when(F.col("ship_date") < F.col("order_date"), None).otherwise(F.col("ship_date"))
            )
    
    return silver_df 


def cleaning_invalid_quantity_value(df):
    
    silver_df = df.filter(F.col("quantity") > 0)
    
    return silver_df

def cleaning_invalid_unit_price_value(df):
    
    silver_df = (
        df.withColumn("unit_price", F.when(F.col("unit_price") <= 0, None).otherwise(F.col("unit_price")))
    )
    
    return silver_df

def cleaning_invalid_discount_pct_value(df):
    
    silver_df = (
        df.withColumn("discount_pct", F.when(F.col("discount_pct") > 100, None).otherwise(F.col("discount_pct")))
    )
    
    return silver_df

def age_cleaning(df):

    silver_df = df.withColumn(
        "customer_age", F.when((F.col("customer_age") < 15) | (F.col("customer_age") > 110), None)
                    .otherwise(F.col("customer_age")))
    
    return silver_df

def gender_cleaning(df):
    
    silver_df = df.withColumn(
    "gender", 
    F.when(F.upper(F.col("gender")) == "MALE", "M")
        .when(F.upper(F.col("gender")) == "FEMALE", "F")
        .when(F.col("gender").isin("M", "F"), F.col("gender"))
        .otherwise(None))
   
    return silver_df
        
def payment_cleaning(df):
    
    silver_df = df.withColumn(
        "payment_type", 
        F.when(F.col("payment_type").isin(["Card", "UPI", "COD"]), F.col("payment_type"))
        .otherwise(None)
    )
    
    return silver_df

def main():
    silver_df_one = drop_duplicates_by_smallest_date(bronze_df)
    silver_df_two = correct_shiping_date(silver_df_one)
    silver_df_three = cleaning_invalid_quantity_value(silver_df_two)
    silver_df_four =  cleaning_invalid_unit_price_value(silver_df_three)
    silver_df_five  =  cleaning_invalid_discount_pct_value(silver_df_four)
    silver_df_six = age_cleaning(silver_df_five)
    silver_df_seven = gender_cleaning(silver_df_six)
    silver_df_eight = payment_cleaning(silver_df_seven)
    
    silver_df_eight.write.mode("overwrite").parquet(str(silver_file_path))

if __name__ == "__main__":
    main()

    
    
    
    
    
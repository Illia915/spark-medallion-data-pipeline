from pathlib import Path 

import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.sql import Window, DataFrame

import utils.config as cfg

def deduplicate_by_earliest_date(df: DataFrame) -> DataFrame:
    """
    Removes all duplicates and keeps only the first record based on date.
    """
    window_spec = Window.partitionBy("transaction_id").orderBy("order_date")
    
    silver_df = (
        df.withColumn("row_num", F.row_number().over(window_spec))\
        .filter(F.col("row_num") == 1)\
        .drop("row_num")
    )      
    
    return silver_df

def validate_shipping_dates(df: DataFrame) -> DataFrame:
    """
    Validates dates to ensure shipping occurs after or on the order date.
    """
    silver_df = df.withColumn(
            "ship_date", F.when(F.col("ship_date") < F.col("order_date"), None).otherwise(F.col("ship_date"))
            )
    
    return silver_df 


def filter_positive_quantities(df: DataFrame) -> DataFrame:
    """
    Keeps only records where the quantity is greater than zero.
    """
    silver_df = df.filter(F.col("quantity") > 0)
    
    return silver_df

def validate_unit_prices(df: DataFrame) -> DataFrame:
    """
    Keeps only records with positive unit price values.
    """
    silver_df = (
        df.withColumn("unit_price", F.when(F.col("unit_price") <= 0, None).otherwise(F.col("unit_price")))
    )
    
    return silver_df

def filter_valid_discounts(df: DataFrame) -> DataFrame:
    """
    Keeps only records where the discount percentage is between 0 and 100.
    """
    silver_df = (
        df.withColumn("discount_pct", F.when(F.col("discount_pct") > 100, None).otherwise(F.col("discount_pct")))
    )
    
    return silver_df

def nullify_invalid_age(df: DataFrame) -> DataFrame:
    """
    Replaces invalid age values with NULL.
    """
    silver_df = df.withColumn(
        "customer_age", F.when((F.col("customer_age") < 15) | (F.col("customer_age") > 110), None)
                    .otherwise(F.col("customer_age")))
    
    return silver_df

def normalize_gender_values(df: DataFrame) -> DataFrame:
    """
    Normalizes values in the gender column (e.g., to 'M' and 'F').
    """
    silver_df = df.withColumn(
    "gender", 
    F.when(F.upper(F.col("gender")) == "MALE", "M")
        .when(F.upper(F.col("gender")) == "FEMALE", "F")
        .when(F.col("gender").isin("M", "F"), F.col("gender"))
        .otherwise(None))
   
    return silver_df
        
def filter_allowed_payment_types(df: DataFrame) -> DataFrame:
    """
    Replaces values not found in the predefined list with NULL.
    """
    silver_df = df.withColumn(
        "payment_type", 
        F.when(F.col("payment_type").isin(["Card", "UPI", "COD"]), F.col("payment_type"))
        .otherwise(None)
    )
    
    return silver_df

def main():
    spark = cfg.create_session("BronzeToSilver")

    bronze_df = (spark.read.parquet(str(cfg.BRONZE_RETAIL_SOURCE)))
    
    silver_df = (
        bronze_df
        .transform(deduplicate_by_earliest_date)
        .transform(validate_shipping_dates)
        .transform(filter_positive_quantities)
        .transform(validate_unit_prices)
        .transform(filter_valid_discounts)
        .transform(nullify_invalid_age)
        .transform(normalize_gender_values)
        .transform(filter_allowed_payment_types)
    )
    
    silver_df.write.mode("overwrite").parquet(str(cfg.SILVER_SINK))

if __name__ == "__main__":
    main()

    
    
    
    
    
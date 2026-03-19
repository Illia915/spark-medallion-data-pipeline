from pathlib import Path

import pyspark.sql.functions as F 
from pyspark.sql import SparkSession, DataFrame

import utils.config as cfg 

def gold_daily_sales_info(df: DataFrame) -> DataFrame:
    """
    Aggregates daily sales data.
    
    Calculates:
    - Total revenue (rounded to 2 decimal places)
    - Total number of orders
    - Average order value
    """
    return (
        df.groupBy("order_date")
        .agg(
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.count("transaction_id").alias("orders_count"),
            F.round(F.avg("total_amount"), 2).alias("avg_order_value")
        )
    )

def gold_product_performance_info(df: DataFrame) -> DataFrame:
    """
    Analyzes performance by product category.

    Calculates:
    - Units sold per category
    - Total revenue per category
    - Average customer age per category
    - Total order count per category
    """
    return (
        df.groupBy("product_category")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.round(F.sum("total_amount"), 2).alias("category_revenue"),
            F.round(F.avg("customer_age"), 2).alias("avg_customer_age"),
            F.count("transaction_id").alias("orders_count")
        )
    )

def gold_city_revenue_info(df: DataFrame) -> DataFrame:
    """
    Analyzes revenue and delivery metrics by city.
    
    Calculates:
    - Total order count per city
    - Total revenue per city
    - Average delivery time (days/hours)
    - Average order value
    """
    return (
        df.groupBy("city")
        .agg(
            F.count("transaction_id").alias("orders_count"),
            F.round(F.sum("total_amount"), 2).alias("city_revenue"),
            F.round(F.avg("delivery_time"), 2).alias("avg_delivery_time"),
            F.round(F.avg("total_amount"), 2).alias("avg_order_value")
        )
    )

def main():
    
    spark = cfg.create_session('SilverToGold')

    silver_df = (
        spark.read
        .parquet(str(cfg.SILVER_RETAIL_SOURCE))
    )
    
    enriched_df = silver_df.withColumn(
        "total_amount", 
        F.round(F.col("quantity") * F.col("unit_price") * (1 - F.coalesce(F.col("discount_pct"), F.lit(0))/ 100), 2)
    ).withColumn(
        "delivery_time", 
        F.datediff(F.col("ship_date"), F.col("order_date"))
    )
    
    daily_sales_df = gold_daily_sales_info(enriched_df)
    daily_sales_df.write.mode("overwrite").parquet(str(cfg.GOLD_SINK / f"daily_sales.parquet"))

    
    product_performance_df = gold_product_performance_info(enriched_df)
    product_performance_df.write.mode("overwrite").parquet(str(cfg.GOLD_SINK / f"product_performance.parquet"))

    city_revenue_analyz_df = gold_city_revenue_info(enriched_df)
    city_revenue_analyz_df.write.mode("overwrite").parquet(str(cfg.GOLD_SINK/ f"city_revenue.parquet"))
    
if __name__ == "__main__":
    main()

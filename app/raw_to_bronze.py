import utils.config as cfg
from schemas.bronze_table_schema import retail_sales_bronze


def main():
    spark = cfg.create_session('RawToBronze')

    bronze_df = (
        spark.read
        .schema(retail_sales_bronze)
        .option("header", True)
        .csv(str(cfg.RAW_RETAIL_SOURCE))
    )    
    
    (bronze_df.write
        .mode("overwrite")
        .parquet(str(cfg.BRONZE_SINK)))
     
if __name__ == "__main__":
    main()
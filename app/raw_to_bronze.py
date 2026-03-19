import sys 
from pathlib import Path

from pyspark.sql import SparkSession

spark = (   
    SparkSession.builder\
        .appName('RawToBronze')\
        .getOrCreate()
)

SPARK_APP = Path('/opt/spark-app')
SPARK_DATA = Path('/opt/spark-data')

raw_file_path = SPARK_DATA / "raw" / "retail_sales_raw.csv"
bronze_file_path = SPARK_DATA / "bronze" 
if SPARK_APP not in sys.path:
    sys.path.insert(0, str(SPARK_DATA))
    
from schemas.bronze_table_schema import retail_sales_bronze

bronze_df = (
    spark.read\
    .schema(retail_sales_bronze)
    .option("header", True)
    .csv(str(raw_file_path))
)


(
    bronze_df.write\
        .mode("overwrite")\
        .parquet(str(bronze_file_path))
)          
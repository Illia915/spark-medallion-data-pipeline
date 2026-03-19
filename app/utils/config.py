from pathlib import Path
from pyspark.sql import SparkSession

def create_session(app_name: str) -> SparkSession:
    """
    Create and return SparkSession object

    Args:
        app_name: Name of Spark application.

    Returns:
        SparkSession object.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    ) 
    
BASE_APP_DIR = Path('/opt/spark-app')
BASE_DATA_DIR = Path('/opt/spark-data')

# Шляхи для читання (Source/Targets)
RAW_RETAIL_SOURCE    = BASE_DATA_DIR / "raw/retail_sales_raw.csv"
BRONZE_RETAIL_SOURCE = BASE_DATA_DIR / "bronze/*.parquet"
SILVER_RETAIL_SOURCE = BASE_DATA_DIR / "silver/*.parquet"

# Шляхи для запису (Sinks/Targets)
BRONZE_SINK = BASE_DATA_DIR / "bronze"
SILVER_SINK = BASE_DATA_DIR / "silver"
GOLD_SINK   = BASE_DATA_DIR / "gold"

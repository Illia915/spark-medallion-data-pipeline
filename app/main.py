import time
import sys
from datetime import datetime

from utils.logger import get_logger
import raw_to_bronze as rtb
import bronze_to_silver as bts 
import silver_to_gold as stg

logger = get_logger(__name__)

def main():
    start_pipeline_time = time.time()
    today_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    logger.info(f"==== Pipeline Started: {today_str} ====")
    
    stages = [
        ("Raw to Bronze", rtb.main),
        ("Bronze to Silver", bts.main),
        ("Silver to Gold", stg.main)
    ]

    try:
        for stage_name, stage_func in stages:
            stage_start = time.time()
            logger.info(f"--- Starting stage: {stage_name} ---")
            
            stage_func()
            
            stage_duration = round(time.time() - stage_start, 2)
            logger.info(f"Successfully finished {stage_name} in {stage_duration}s")

        total_duration = round(time.time() - start_pipeline_time, 2)
        logger.info(f"==== Pipeline Ended Successfully. Total duration: {total_duration}s ====")

    except Exception as e:
        logger.error(f"==== Pipeline CRASHED at stage '{stage_name}' ====")
        logger.error(f"Error message: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
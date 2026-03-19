import logging
import sys
from pathlib import Path

def get_logger(name: str):
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        logger.setLevel(logging.INFO)

        log_dir = Path("logs")
        
        log_dir.mkdir(parents=True, exist_ok=True)
        
        log_file_path = log_dir / "app.log"

        formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')

        file_handler = logging.FileHandler(log_file_path, mode='a', encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger
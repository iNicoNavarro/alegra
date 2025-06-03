# src/utils/trackers.py
import json, logging
from pathlib import Path
from pyspark.sql import DataFrame
from utils.spark_helpers import save_parquet


def setup_logging(log_path, logger_name="etl_logger"):
    log_dir = Path(log_path)
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    if logger.hasHandlers(): 
        logger.handlers.clear() #TODO: in production, dont clear

    log_file = log_dir / "etl.log"
    file_handler = logging.FileHandler(
        filename=str(log_file),
        mode="a", 
        encoding="utf-8"
    )
    file_handler.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.info(f"Logging initialized at {log_file}")
    return logger


def log_and_register(
        table: str, 
        df: DataFrame, 
        output_path: str, 
        load_date: str, 
        metadata: dict,
        logger: object
    ) -> dict:
    count = df.count()
    
    save_parquet(
        data=df,
        path=output_path,
        logger=logger
    )
    # df.write.mode("overwrite").parquet(output_path)
    logger.info(f"[{table}] {count} records written to {output_path}")
    
    metadata[table] = {
        "path": output_path,
        "load_date": load_date,
        "record_count": count,
    }
    return metadata


def save_metadata(
        step_name: str, 
        metadata: dict, 
        metadata_path: str,
        logger: object
    ):
    metadata_dir = Path(metadata_path)
    metadata_dir.mkdir(parents=True, exist_ok=True)
    file_path=f"{metadata_dir}/{step_name}.json"
    with open(file_path, "w") as f:
        json.dump(metadata, f, indent=2)

    logger.info(f"Metadata for '{step_name}' saved to {file_path}")


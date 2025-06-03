import logging
import json
from typing import Dict, Any
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException

def load_metadata(metadata_path: str) -> Dict[str, Any]:
    path = Path(metadata_path)
    if not path.exists():
        raise FileNotFoundError(f"Metadata file not found: {metadata_path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_metadata(metadata: Dict[str, Any], metadata_path: str) -> None:
    path = Path(metadata_path)
    if not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)


def execute_query(
        spark: SparkSession, 
        path: str, 
        logger: logging.Logger = None
) -> DataFrame:
    try:
        with open(path, "r", encoding="utf-8") as f:
            query = f.read()
        if logger:
            logger.info(f"Executing SQL query from file: {path}")
            logger.debug(f"Query content:\n{query}")
        return spark.sql(query)
    except AnalysisException as ae:
        if logger:
            logger.exception(f"Analysis error when executing SQL query: {ae}")
        raise
    except Exception:
        if logger:
            logger.exception("Error executing SQL query.")
        raise


def load_parquet(
        spark: SparkSession, 
        path: str, 
        logger: logging.Logger = None
) -> DataFrame:
    try:
        if logger:
            logger.info(f"Loading data from Parquet: {path}")
        df = spark.read.parquet(path)
        return df
    except AnalysisException as ae:
        if logger:
            logger.exception(f"Error reading Parquet ({path}): {ae}")
        raise
    except Exception as e:
        if logger:
            logger.exception(f"Error loading Parquet file {path}: {e}")
        raise


def save_parquet(
    data: DataFrame,
    path: str,
    mode: str = "overwrite",
    repartition: int = 4,
    logger: logging.Logger = None
):
    try:
        target_dir = Path(path)
        target_dir.mkdir(parents=True, exist_ok=True)

        data_to_write = data.repartition(repartition)
        data_to_write.write.mode(mode).parquet(str(target_dir))
        if logger:
            logger.info(f"DataFrame successfully saved to: {path}")
    except Exception:
        if logger:
            logger.exception(f"Error saving DataFrame to: {path}")
        raise


def register_temp_view(
    df: DataFrame, 
    name: str, 
    logger: logging.Logger = None
    ):
    try:
        df.createOrReplaceTempView(name)
        if logger:
            logger.info(f"Temporary view registered: {name}")
    except Exception:
        if logger:
            logger.exception(f"Error registering temporary view: {name}")
        raise

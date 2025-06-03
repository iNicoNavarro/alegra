import abc
from pyspark.sql import DataFrame, SparkSession
from typing import Any, Dict, Optional

class BaseConnector(abc.ABC):

    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        self.spark = spark

    @abc.abstractmethod
    def connect(self) -> None:
        pass

    @abc.abstractmethod
    def close(self) -> None:
        pass

    @abc.abstractmethod
    def execute_sql(self, sql: str, options: Optional[Dict[str, Any]] = None) -> DataFrame:
        pass

    @abc.abstractmethod
    def write_dataframe(
        self,
        df: DataFrame,
        table: str,
        mode: str = "overwrite",
        options: Optional[Dict[str, Any]] = None
    ) -> None:
        pass

import os
import logging
import textwrap
from pathlib import Path
from typing import Any, Dict, Optional, List, Union

import pyodbc                           # ← NEW
from pyspark.sql import DataFrame, SparkSession
from utils.connections.base_connector import BaseConnector

logger = logging.getLogger("SQLServerConnector")


class SQLServerConnector(BaseConnector):
    def __init__(
        self,
        spark: SparkSession,
        config: Optional[Dict[str, Any]] = None,
        *,
        env_prefix: str = "SQLSERVER",
        logger: logging.Logger = None,
    ) -> None:
        super().__init__(spark, config)

        self.logger = logger or logging.getLogger("SQLServerConnector")

        if config is None:
            self.host     = os.getenv(f"{env_prefix}_HOST")
            self.port     = os.getenv(f"{env_prefix}_PORT", "1433")
            self.database = os.getenv(f"{env_prefix}_DATABASE")
            self.user     = os.getenv(f"{env_prefix}_USER")
            self.password = os.getenv(f"{env_prefix}_PASSWORD")
            encrypt       = os.getenv(f"{env_prefix}_ENCRYPT", "false").lower() in ("true", "1", "yes")
        else:
            self.host     = config.get("host")
            self.port     = str(config.get("port", "1433"))
            self.database = config.get("database")
            self.user     = config.get("user")
            self.password = config.get("password")
            encrypt       = bool(config.get("encrypt", False))

        missing = [k for k in ["host", "port", "database", "user", "password"] if not getattr(self, k)]
        if missing:
            raise ValueError(f"Missing SQL Server configuration for: {', '.join(missing)}")

        encrypt_str = "true" if encrypt else "false"
        self.jdbc_url = (
            f"jdbc:sqlserver://{self.host}:{self.port};"
            f"databaseName={self.database};"
            f"user={self.user};password={self.password};encrypt={encrypt_str};"
        )
        self.driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

        #  DML Connect
        self._pyodbc = pyodbc.connect(
            # usa el que realmente está instalado:
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.host},{self.port};DATABASE={self.database};"
            f"UID={self.user};PWD={self.password};TrustServerCertificate=yes;"
        )
        self._pyodbc.autocommit = True
        self.logger.info(f"Initialized SQLServerConnector for DB '{self.database}' at {self.host}:{self.port}")


    def __enter__(self) -> "SQLServerConnector":
        return self


    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


    def connect(self) -> None:
        self.logger.info("SQLServerConnector.connect() called (no-op for Spark JDBC)")


    def close(self) -> None:
        self.logger.info("SQLServerConnector.close() called")
        self._pyodbc.close()


    def execute_sql(
            self,
            sql: str,
            as_select: bool = True,
            options: Optional[Dict[str, Any]] = None
    ) -> Optional[DataFrame]:
        """
        • as_select=True  → devuelve DataFrame (SELECT o consulta que retorna filas)
        • as_select=False → ejecuta DML/DDL y devuelve None
        """
        if as_select:
            opts = {
                "url": self.jdbc_url,
                "query": textwrap.dedent(sql),
                "driver": self.driver,
            }
            if options:
                opts.update(options)
            self.logger.debug("→ Spark JDBC query\n%s", sql)
            return self.spark.read.format("jdbc").options(**opts).load()

        # DML/DDL: usa pyodbc y no devuelve DataFrame
        self.logger.debug("→ pyodbc exec\n%s", sql)
        cur = self._pyodbc.cursor()
        cur.execute(sql)
        cur.close()
        return None



    def write_dataframe(
        self,
        df: DataFrame,
        table: str,
        mode: str = "overwrite",
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        opts = {
            "url": self.jdbc_url,
            "dbtable": table,
            "driver": self.driver,
        }
        if options:
            opts.update(options)
        self.logger.info(f"→ Writing DataFrame to {table} (mode={mode})")
        df.write.format("jdbc").options(**opts).mode(mode).save()


    def drop_table(self, table: str) -> None:
        self.execute_sql(f"DROP TABLE IF EXISTS {table}", as_select=False)


    def upsert(
            self,
            df: DataFrame,
            target: str,
            key_columns: List[str],
            staging: str,
            merge_sql_path: Optional[Union[str, Path]] = None,
            batchsize: int = 1000,
    ) -> None:
        # ── 1. subir staging ────────────────────────────────────────────
        self.logger.info(f"→ Writing staging to {staging}")
        self.write_dataframe(df, table=staging, mode="overwrite",
                            options={"batchsize": batchsize})
        self.logger.info(f"{df.count():,} rows written to staging")

        # ── 2. localizar plantilla MERGE ────────────────────────────────
        merge_sql_path = (
            Path(f"sql/{target.replace('.', '_')}_merge.sql")
            if merge_sql_path is None else Path(merge_sql_path)
        )
        if not merge_sql_path.exists():
            raise FileNotFoundError(f"MERGE SQL file not found: {merge_sql_path}")

        # ── 3. renderizar plantilla ────────────────────────────────────
        merge_sql = merge_sql_path.read_text(encoding="utf-8").format(
            staging=staging,
            target=target,
            keys=",".join(key_columns),
        )

        # Asegura punto-y-coma final (SQL Server lo exige dentro de sp_executesql)
        if not merge_sql.rstrip().endswith(";"):
            merge_sql = merge_sql.rstrip() + ";"

        # ── 4. envolver en EXEC sp_executesql (escape de comillas) ─────
        escaped_sql = merge_sql.replace("'", "''")
        wrapped_sql = f"EXEC sp_executesql N'{escaped_sql}'"

        # ── 5. ejecutar via pyodbc ──────────────────────────────────────
        self.logger.debug(f"MERGE script:\n{merge_sql}")
        self.logger.info(f"→ Executing MERGE into {target}")
        self.execute_sql(wrapped_sql, as_select=False)
        self.logger.info("   ▸ MERGE completed")


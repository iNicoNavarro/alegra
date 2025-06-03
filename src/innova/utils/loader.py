import logging
from pathlib import Path

from typing import Dict, Any, List, Set

from pyspark.sql import SparkSession, DataFrame, functions as F
from utils.connections.sql_server_connector import SQLServerConnector


class DataWarehouseLoader:
    def __init__(
        self,
        spark: SparkSession,
        sql_conn: SQLServerConnector,
        metadata: Dict[str, Any],
        dwh_cfg: Dict[str, Any],
        logger: logging.Logger | None = None,
    ) -> None:
        self.spark = spark
        self.sql = sql_conn
        self.meta = metadata
        self.cfg = dwh_cfg
        self.schema = dwh_cfg["schema"]
        self.logger = logger or logging.getLogger("DataWarehouseLoader")
        self.logger.setLevel(logging.INFO)
        self._dim_cache: Dict[str, DataFrame] = {}


    def run(self) -> None:
        order = self._topological_order()
        self.logger.info(f"→ Orden de carga DWH: {order}")
        for entity in self._topological_order():
            cfg = self.cfg.get(entity) or {}
            if cfg.get("managed_externally"):
                self._dim_cache[entity] = self._load_dim_from_db(entity)
                continue
            if entity.startswith("dim_"):
                if "fk_map" in cfg:
                    self._load_dim(entity, cfg)
                else:
                    self._load_lookup_dim(entity, cfg)
            elif entity.startswith("fact_"):
                self._load_fact(entity, cfg)
        self.logger.info("DWH load completed.")


    def _load_lookup_dim(self, dim_name: str, cfg: Dict[str, Any]) -> None:
        df_src = self._read_source(cfg["source"])
        df_dim = self._select_and_alias(df_src, cfg)

        natural = cfg["natural_keys"][0]   
        df_dim = df_dim.dropDuplicates([natural])

        for col, val in cfg.get("defaults", {}).items():
            lit_col = F.lit(val)
            if val is None:
                lit_col = lit_col.cast("string")
            df_dim = df_dim.withColumn(col, lit_col)

        self._upsert(df_dim, dim_name, cfg)



    def _load_dim(self, dim_name: str, cfg: Dict[str, Any]) -> None:
        df_src = self._read_source(cfg["source"])
        df_dim = self._select_and_alias(df_src, cfg)
        df_dim = self._resolve_fk(df_dim, cfg.get("fk_map", {}))
        self._upsert(df_dim, dim_name, cfg)


    def _load_fact(self, fact_name: str, cfg: Dict[str, Any]) -> None:
        df_src = self._read_source(cfg["source"])
        df_fact = self._select_and_alias(df_src, cfg)
        df_fact = self._resolve_fk(df_fact, cfg.get("fk_map", {}))
        df_fact = df_fact.withColumn("created_at", F.current_timestamp())
        self._upsert(df_fact, fact_name, cfg)


    def _read_source(self, source_key: str) -> DataFrame:
        for entry in self.meta.values():
            db_tbl = entry.get("db_table")
            if db_tbl and db_tbl.split(".")[-1] == source_key:
                self.logger.info(f"Leyendo tabla staging {db_tbl}")
                return self.sql.execute_sql(f"SELECT * FROM {db_tbl}")

        if source_key in self.meta:
            path = self.meta[source_key]["path"]
            self.logger.info(f"Leyendo Parquet {path}")
            return self.spark.read.parquet(path)

        full_tbl = f"{self.schema}.{source_key}"
        self.logger.info(f"Leyendo tabla {full_tbl}")
        return self.sql.execute_sql(f"SELECT * FROM {full_tbl}")


    def _select_and_alias(self, df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
        for src, tgt in cfg.get("select", {}).items():
            df = df.withColumnRenamed(src, tgt)
        return df.select(list(cfg["select"].values()))


    def _resolve_fk(self, df: DataFrame, fk_map: Dict[str, Any]) -> DataFrame:

        for fk_col, rule in fk_map.items():
            if "expr" in rule:
                df = df.withColumn(fk_col, F.expr(rule["expr"]))
            else:
                dim_name  = rule["dim"]
                join_cond = rule["join"]   # Ej: "category_raw = category_name"
                left_expr, right_expr = map(str.strip, join_cond.split("="))

                # Traemos la dimensión completa (para cachearla), pero en el join
                # solo usaremos el surrogate key y la columna de join (right_expr).
                df_dim_full = self._dim_cache.get(dim_name)
                if df_dim_full is None:
                    df_dim_full = self._load_dim_from_db(dim_name)
                    self._dim_cache[dim_name] = df_dim_full

                # Nombre del surrogate key en la tabla dwh, p. ej. "id_category"
                id_col = rule.get("id_col") or f"id_{dim_name.split('_', 1)[1]}"

                # Creamos un pequeño DataFrame que sólo lleve (id_col, right_expr)
                # para evitar arrastrar created_at, updated_at, deleted_at, etc.
                df_dim_trimmed = df_dim_full.select(id_col, right_expr)

                # Hacemos el join contra ese mini‐DataFrame
                df = df.join(df_dim_trimmed,
                             df[left_expr] == df_dim_trimmed[right_expr],
                             how="left")\
                       .drop(right_expr)  \
                       .withColumnRenamed(id_col, fk_col)

        return df



    def _upsert(self, df: DataFrame, entity: str, cfg: Dict[str, Any]) -> None:
        target_tbl  = f"{self.schema}.{entity}"
        staging_tbl = f"stg_{entity}"
        key_cols    = cfg["natural_keys"]

        sql_dir = self.cfg.get("sql_dir", "sql")
        merge_sql_path = Path(sql_dir) / f"{target_tbl.replace('.', '_')}_merge.sql"

        self.sql.upsert(
            df=df,
            target=target_tbl,
            key_columns=key_cols,
            staging=staging_tbl,
            merge_sql_path=merge_sql_path,   # ← aquí
            batchsize=1000,
        )
        if entity.startswith("dim_"):
            self._dim_cache[entity] = self._load_dim_from_db(entity)


    def _load_dim_from_db(self, dim_name: str) -> DataFrame:
        self.logger.info(f"Refrescando cache {dim_name}")
        return self.sql.execute_sql(f"SELECT * FROM {self.schema}.{dim_name}")


    def _topological_order(self) -> List[str]:
        nodes = [k for k in self.cfg if k.startswith(("dim_", "fact_"))]
        edges: Dict[str, Set[str]] = {n: set() for n in nodes}
        for name in nodes:
            cfg_entry = self.cfg[name]
            if not isinstance(cfg_entry, dict):
                self.logger.warning(f"Omitiendo '{name}': entrada no es dict")
                continue
            for fk in cfg_entry.get("fk_map", {}).values():
                dim_dep = fk.get("dim")
                if dim_dep:
                    edges[name].add(dim_dep)
        visited, stack, order = set(), set(), []


        def visit(n: str):
            if n in visited:
                return
            if n in stack:
                raise ValueError("Dependencia circular en configuración DWH")
            stack.add(n)
            for dep in edges[n]:
                visit(dep)
            stack.remove(n)
            visited.add(n)
            order.append(n)

        for n in nodes:
            visit(n)
        return order

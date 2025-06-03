from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    StringType, LongType, IntegerType, DateType, DecimalType
)

def apply_strict_schema(df: DataFrame, schema_dict: dict) -> DataFrame:
    for col_name, type_str in schema_dict.items():
        if type_str.startswith("StringType"):
            df = df.withColumn(col_name, F.col(col_name).cast(StringType()))
        elif type_str.startswith("LongType"):
            df = df.withColumn(col_name, F.col(col_name).cast(LongType()))
        elif type_str.startswith("IntegerType"):
            df = df.withColumn(col_name, F.col(col_name).cast(IntegerType()))
        elif type_str.startswith("DateType"):
            df = df.withColumn(col_name, F.to_date(F.col(col_name), "yyyy-MM-dd"))
        elif type_str.startswith("DecimalType"):
            inside = type_str.strip().replace("DecimalType", "").strip("()")
            prec, scale = map(int, inside.split(","))
            df = df.withColumn(col_name, F.col(col_name).cast(DecimalType(prec, scale)))
        else:
            #TODO: add more types cast
            pass
    return df

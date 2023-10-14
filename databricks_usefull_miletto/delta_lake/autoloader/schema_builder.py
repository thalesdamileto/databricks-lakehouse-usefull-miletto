from typing import Union
import json

from pyspark.sql.types import *

from databricks_usefull_miletto.domain.metadata_class import PipelineConfigs

"""To understand the column mapping used here, please check 
databricks_usefull_miletto.infra.config.settings.column_mapping_sample"""


def sql_python_type_dict(precision1: int = 10, precision2: int = 4) -> dict:
    sql_python_types = [
        {"sql_type": "int", "python_type": "int32", "spark_type": IntegerType()},
        {"sql_type": "varchar", "python_type": "str", "spark_type": StringType()},
        {"sql_type": "char", "python_type": "str", "spark_type": StringType()},
        {"sql_type": "text", "python_type": "str", "spark_type": StringType()},
        {"sql_type": "nvarchar", "python_type": "str", "spark_type": StringType()},
        {"sql_type": "nchar", "python_type": "str", "spark_type": StringType()},
        {"sql_type": "ntext", "python_type": "str", "spark_type": StringType()},
        {"sql_type": "date", "python_type": "datetime", "spark_type": TimestampType()},
        {
            "sql_type": "datetime",
            "python_type": "datetime",
            "spark_type": TimestampType(),
        },
        {
            "sql_type": "datetime2",
            "python_type": "datetime",
            "spark_type": TimestampType(),
        },
        {
            "sql_type": "smalldatetime",
            "python_type": "datetime",
            "spark_type": TimestampType(),
        },
        {"sql_type": "bigint", "python_type": "float64", "spark_type": LongType()},
        {"sql_type": "bit", "python_type": "bool", "spark_type": BooleanType()},
        {
            "sql_type": "decimal",
            "python_type": "float64",
            "spark_type": DecimalType(precision1, precision2),
        },
        {
            "sql_type": "money",
            "python_type": "float64",
            "spark_type": DecimalType(precision1, precision2),
        },
        {
            "sql_type": "numeric",
            "python_type": "float64",
            "spark_type": DecimalType(precision1, precision2),
        },
        {"sql_type": "smallint", "python_type": "int32", "spark_type": ShortType()},
        {
            "sql_type": "smallmoney",
            "python_type": "float64",
            "spark_type": DecimalType(precision1, precision2),
        },
        {"sql_type": "tinyint", "python_type": "int32", "spark_type": ShortType()},
        {"sql_type": "float", "python_type": "float64", "spark_type": DoubleType()},
        {"sql_type": "real", "python_type": "float64", "spark_type": FloatType()},
        {
            "sql_type": "uniqueidentifier",
            "python_type": "int32",
            "spark_type": StringType(),
        },
    ]
    return sql_python_types


def schema_get_type(sql_type: str, desired_type: str) -> Union[DataType, str]:
    if "(" in sql_type:
        s_type = sql_type.split("(")
        precision = s_type[1]
        s_type = s_type[0]
        precision = precision.split(",")
        precision1 = precision[0]
        precision2 = precision[1].replace(")", "")
        sql_type = s_type
        sql_python_types = sql_python_type_dict(int(precision1), int(precision2))

    else:
        sql_python_types = sql_python_type_dict()

    for parse_types in sql_python_types:
        if sql_type == parse_types["sql_type"]:
            output_type = parse_types[desired_type]
            return output_type

    return "NotFound"


def build_schema_from_mapping(pipe_settings: PipelineConfigs, output_type: str) -> StructType:
    column_mapping = pipe_settings.metadata.column_mapping
    operation_id = pipe_settings.operation_id

    columns_data = []
    for key in json.loads(column_mapping):
        column = {
            "source_column": key["sc"].replace(" ", "_").upper(),
            "type": key["t"],
            "output_type": schema_get_type(key["t"], output_type),
        }
        columns_data.append(column)

    schema_list = []
    # adding_extra_changetracking_columns
    if operation_id == 3:
        schema_list.append(StructField("SYS_CHANGE_OPERATION", StringType(), True))
        pk_columns = pipe_settings.metadata.pk_column
        pk_columns_list = str.upper(pk_columns).split(",")
        pk_columns_list = [pkcolumn + "_CT" for pkcolumn in pk_columns_list]
        for i in pk_columns_list:
            for x in columns_data:
                if i == x["source_column"] + "_CT":
                    schema_list.append(StructField(str(i), x["output_type"], True))

    for column_info in columns_data:
        sc = column_info["source_column"]
        spark_type = column_info["output_type"]
        schema_list.append(StructField(str(sc), spark_type, True))

    schema = StructType(schema_list)

    return schema

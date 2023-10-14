from pyspark.sql.types import *


def sql_python_type_dict(precision1: int = 10, precision2: int = 0) -> dict:
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


def schema_get_type(sql_type, desired_type) -> Union[DataType, str]:
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

    for x in sql_python_types:
        if sql_type == x["sql_type"]:
            output_type = x[desired_type]
            return output_type

    return "NotFound"


def build_schema_from_mapping(sdid: BatchSettings, output_type: str) -> StructType:
    column_mapping = sdid.metadata["ColumnMapping"]
    operation_id = sdid.operation_id

    columns_data = []
    for i in json.loads(column_mapping):
        column = {
            "source_column": i["sc"].replace(" ", "_").upper(),
            "type": i["t"],
            "output_type": schema_get_type(i["t"], output_type),
        }
        columns_data.append(column)

    schema_list = []
    # add_extra_changetracking_columns
    if operation_id == 3:
        schema_list.append(StructField("SYS_CHANGE_OPERATION", StringType(), True))
        pk_columns = sdid.metadata["PKColumn"]
        pk_columns_list = str.upper(pk_columns).split(",")
        pk_columns_list = [pkcolumn + "_CT" for pkcolumn in pk_columns_list]
        for i in pk_columns_list:
            for x in columns_data:
                if i == x["source_column"] + "_CT":
                    schema_list.append(StructField(str(i), x["output_type"], True))

    for i in columns_data:
        sc = i["source_column"]
        spark_type = i["output_type"]
        schema_list.append(StructField(str(sc), spark_type, True))

    schema = StructType(schema_list)

    return schema
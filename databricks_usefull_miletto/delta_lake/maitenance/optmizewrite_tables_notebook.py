## Objective  Notebook to be used as optimization and vaccum process
## Use in workflows with parameters Catalog and Schema
## Use * on catalog or schema if you want to processo for all schemas within your catalog ("*" only works with UC)
## Author Thales Morais - Adapted from Ricardo Conegliam



# LOADING PARAMETERS
dbutils.widgets.text("catalog","main")
dbutils.widgets.text("schema","schema01")
dbutils.widgets.dropdown("catalogType",defaultValue="UC",choices=["UC","LEGACY"])

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
catalogType = dbutils.widgets.get("catalogType")

# catalog = "main"
# schema = "schema01"



# IMPORTING "THINGS"
from delta import DeltaTable
from pyspark.sql.functions import col,lit



# OPTIMIZATION FUNCTION
# This function will run Optimize end Vacumn for given catalog and schema

# Note:  Be aware you are not zordering anything!

def OptimizeWriteTables(catalog,schema):

    ## Note : if you are sadly not using UC, use this dataframe definition instead ##
    if catalogType == "LEGACY":
        df = spark.sql(f"show tables from {catalog}.{schema}").select(col("tableName").alias("table_name"),lit(schema).alias("table_schema"),lit(catalog).alias("table_catalog"))
    else:
        df = (spark.table("system.information_schema.tables").
          select("table_catalog","table_schema","table_name")
          .where(f'(table_catalog = "{catalog}" or "{catalog}"="*") and (table_schema = "{schema}" or "{schema}"= "*")')
          .where("data_source_format = 'DELTA'")
          .where("table_catalog <> '__databricks_internal'")
          .orderBy("table_schema"))


    tableList = [data for data in df.select(col("table_catalog"),col("table_schema"),col("table_name")).collect()]
    tam_lista = len(tableList)
    print(f"tamanho lista {tam_lista}")

    i=0
    for table in tableList:
        i += 1
        fullname = f"{table['table_catalog']}.`{table['table_schema']}`.{table['table_name']}"

        print(f"Setting OptimizeWrite for {fullname}... {i}/{tam_lista}")

        try:
            spark.sql(f"ALTER TABLE {fullname} SET TBLPROPERTIES(delta.autoOptimize.optimizeWrite = 'true');")
        except Exception as e:
            output = f"{e}"
            print(f"Error on set OptimizeWrite {fullname} ")



# Actually calling function to organize
OptimizeWriteTables(catalog,schema)

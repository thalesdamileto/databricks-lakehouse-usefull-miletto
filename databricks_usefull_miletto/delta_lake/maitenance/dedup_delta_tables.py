# BASED ON: https://stackoverflow.com/questions/61674476/how-to-drop-duplicates-in-delta-table
# DEVELOPED BY THALES MORAIS


######################## FIELD 1 ########################
catalog = '<catalog-name>'
schema = '<schema-name>'
table = '<table-name>'
full_table = f'{catalog}.{schema}.{table}'
pk_columns = 'number_id,key'
order_by_column = 'write_date_utc'
detail_table = spark.sql(f"describe detail {full_table}")
location = detail_table.collect()[0]['location']
location

######################## FIELD 2 ########################
## COUNT TABLE DUPLCIATED ROWS
df_temp = (
    spark.sql(
        f"SELECT *, ROW_NUMBER() OVER (PARTITION BY {pk_columns} ORDER BY {order_by_column} DESC) rn FROM delta.`{location}`")
).filter("rn > 1").drop('rn').distinct()

print(f"Current duplciated rows: {df_temp.count()}")


########################  FIELD 3 ########################
def build_condition_match(pk_columns):
    pk_columns = pk_columns.lower()
    pk_columns = ",".join(pk_column for pk_column in pk_columns.split(","))

    if type(pk_columns) != list:
        pk_columns_list = pk_columns.split(",")

    condition_statement = []
    for pk_column in pk_columns_list:
        statement = "main.{pk_column} = nodups.{pk_column}".format(pk_column=pk_column)
        condition_statement.append(statement)

    final_condition = str(" AND ".join(condition_statement)).replace("[", "").replace("]", "").replace("'", "")
    print(final_condition)
    return final_condition


final_condition = build_condition_match(pk_columns)

######################## FIELD 4 ########################
from delta.tables import *

# Step 1
df_temp = (
    spark.sql(
        f"SELECT *, ROW_NUMBER() OVER (PARTITION BY {pk_columns} ORDER BY {order_by_column} DESC) rn FROM delta.`{location}`")
).filter("rn > 1").drop('rn').distinct()

print(f"Duplcaited rows: {df_temp.count()}")

# Step 2
delta_temp = DeltaTable.forPath(spark, f"{location}")
delta_temp.alias("main").merge(df_temp.alias("nodups"), f"{final_condition}").whenMatchedDelete().execute()

# Step 3
history_table = spark.sql(f"describe HISTORY delta.`{location}`")
version = history_table.collect()[1]['version']
version

# Step 4
df_temp2 = (
    spark.sql(
        f"SELECT *, ROW_NUMBER() OVER (PARTITION BY {pk_columns} ORDER BY {order_by_column} DESC) rn FROM delta.`{location}` VERSION AS OF {version}")
).filter("rn> 1").drop('rn').distinct()

print(f"Number of rows to be reinserted: {dfTemp2.count()}")

# Step 5
delta_temp = DeltaTable.forPath(spark, f"{location}")

delta_temp.alias("main").merge(df_temp2.alias("nodups"), f"{final_condition}").whenNotMatchedInsertAll().execute()

######################## FIELD 5 ########################
## CHECK TABLE DUPLICATED ROWS
df_temp = (
    spark.sql(
        f"SELECT *, ROW_NUMBER() OVER (PARTITION BY {pk_columns} ORDER BY {order_by_column} DESC) rn FROM delta.`{location}`")
).filter("rn > 1").drop('rn').distinct()

print(f"Total of duplicated rows after the process: {df_temp.count()}")


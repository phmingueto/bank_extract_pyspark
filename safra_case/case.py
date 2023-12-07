from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("app").getOrCreate()


extract_df1 = spark.read.text("/caminho/para/ler/movimetacao_02.txt")
extract_df2 = spark.read.text("/caminho/para/ler/movimetacao_03.txt")
opening_balance = spark.read.text("/caminho/para/ler/saldo_incial.txt")

extract = extract_df1.union(extract_df2)
union_df = extract.union(opening_balance)
union_df = union_df.withColumn("data", F.to_date("data", "dd/MM/yyyy"))

window_spec = Window.partitionBy("CPF").orderBy("data")
df = union_df.withColumn("Saldo Diário", F.sum("Movimentacao_dia").over(window_spec))

min_max_dates = df.select(F.min("Data").alias("min_date"), F.max("Data").alias("max_date"))
date_range_df = min_max_dates.select(
    F.expr("sequence(min_date, max_date, interval 1 day)").alias("Data_range")
).selectExpr("explode(Data_range) as Data")

df_complete = df.select("Nome", "CPF").distinct().crossJoin(date_range_df)
df_complete = df_complete.join(df, on=["Nome", "CPF", "Data"], how="left_outer")
df_complete = df_complete.withColumn("Movimentacao_dia", F.last("Movimentacao_dia", True).over(window_spec))
df_complete = df_complete.withColumn("Saldo Diário", F.round("Saldo Diário", 2))

df_select = df_complete.select('Nome', 'CPF', 'Data', 'Saldo Diário')
df_drop = df_select.dropDuplicates()
df_order = df_drop.orderBy("Nome", "Data")

df_complete = df_order.withColumn("Saldo Diário", F.coalesce(F.last("Saldo Diário", True).over(window_spec), F.lit(0)))

display(df_complete)



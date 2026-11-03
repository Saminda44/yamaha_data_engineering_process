# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, FloatType, DecimalType, IntegerType,DateType
import pyspark.sql.functions as F

catalog_name = 'yamaha-catalog'

# COMMAND ----------

df_silver_supersession = spark.table(f'`{catalog_name}`.silver.slv_supersession')
df_silver_supersession.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spareparts Orders

# COMMAND ----------

df_silver_spareparts_orders = spark.table(f'`{catalog_name}`.silver.slv_spareparts_orders')
df_silver_spareparts_orders.show(10)

df_silver_dealers = spark.table(f'`{catalog_name}`.silver.slv_dealers')
df_silver_dealers.show(10)

# COMMAND ----------

# DBTITLE 1,Drop fully-null columns
# Simplest method: one aggregation counts nulls per column, then drop fully-null columns
nulls = df_silver_spareparts_orders.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df_silver_spareparts_orders.columns
]).first().asDict()

total = df_silver_spareparts_orders.count()
for c, n in sorted(nulls.items(), key=lambda kv: -kv[1]):
    if n > 0:
        print(f"{c}: {n} nulls ({n / total:.2%})")

fully_null = [c for c, n in nulls.items() if n == total]
print(f"\nDropping {len(fully_null)} fully-null columns: {fully_null}")

df_spareparts_orders = df_silver_spareparts_orders.drop(*fully_null)
print(f"Columns: {len(df_spareparts_orders.columns)}")

df_spareparts_orders.show(10)

# COMMAND ----------

# DBTITLE 1,Left join orders with dealers
# Left join orders with dealers: Sold-to_Party = Dealer_Code, bring in dealer geography columns
dealer_cols = ['Province', 'District', 'ASE', 'RM', 'Type']

df_orders_dealers = df_silver_spareparts_orders.join(
    df_silver_dealers.select('Dealer_Code', *dealer_cols),
    on=df_silver_spareparts_orders['Sold-to_Party'] == F.col('Dealer_Code'),
    how='left'
)

print(f"Rows: {df_orders_dealers.count():,} | Columns: {len(df_orders_dealers.columns)}")
unmatched = df_orders_dealers.filter(F.col('Dealer_Code').isNull()).count()
print(f"Rows with no matching dealer: {unmatched:,}")

display(df_orders_dealers.select('Sold-to_Party', 'Dealer_Code', *dealer_cols).limit(10))

# COMMAND ----------


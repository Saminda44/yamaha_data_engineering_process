# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, FloatType, DecimalType, IntegerType,DateType
import pyspark.sql.functions as F

catalog_name = 'yamaha-catalog'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unit Sales

# COMMAND ----------

df_bronze_unit_sales = spark.table(f'`{catalog_name}`.bronze.brz_unit_sales')
df_bronze_unit_sales.show(10)

# cast the date columns to Date type (sources: Billing Date = dd.MM.yyyy, Date = M/d/yy)
df_silver_unit_sales = (df_bronze_unit_sales
    .withColumn("Billing Date", F.to_date("Billing Date", "dd.MM.yyyy"))
    .withColumn("Date", F.to_date("Date", "M/d/yy"))
)

#df_silver_unit_sales.show(10)

# SlsVolQty arrives as a decimal-formatted string (e.g. "1.00"), so cast via double to int
df_silver_unit_sales = df_silver_unit_sales.withColumn("SlsVolQty", F.col("SlsVolQty").cast("double").cast("int"))
#df_silver_unit_sales.show(10)

# money columns contain thousands separators (e.g. "7,010.00") — strip commas before casting to double
df_silver_unit_sales = (df_silver_unit_sales
    .withColumn("Ad Payment", F.regexp_replace("Ad Payment", ",", "").cast("double"))
    .withColumn("Sales Price", F.regexp_replace("Sales Price", ",", "").cast("double"))
    .withColumn("Surcharge", F.regexp_replace("Surcharge", ",", "").cast("double"))
    .withColumn("Discount", F.regexp_replace("Discount", ",", "").cast("double"))
    .withColumn("Net Sales", F.regexp_replace("Net Sales", ",", "").cast("double"))
    .withColumn("Tax Amount", F.regexp_replace("Tax Amount", ",", "").cast("double"))
    .withColumn("Revenue Re", F.regexp_replace("Revenue Re", ",", "").cast("double"))
    .withColumn("Cost Reduc", F.regexp_replace("Cost Reduc", ",", "").cast("double"))
    .withColumn("Cost", F.regexp_replace("Cost", ",", "").cast("double"))
    .withColumn("Net Sales4", F.regexp_replace("Net Sales4", ",", "").cast("double"))
)

df_silver_unit_sales.show(10)

# rename only the columns that still contain spaces/commas/dots — already-clean names stay untouched
df_silver_unit_sales = df_silver_unit_sales.toDF(*[
    c.replace(" ", "_").replace(",", "_").replace(".", "_") for c in df_silver_unit_sales.columns
])

print(df_silver_unit_sales.columns)

df_silver_unit_sales.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"`{catalog_name}`.silver.slv_unit_sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ### UIO

# COMMAND ----------

df_bronze_uio = spark.table(f'`{catalog_name}`.bronze.brz_uio')
df_bronze_uio.show(10)

# rename only the columns that still contain spaces/commas/dots — already-clean names stay untouched
df_silver_uio = df_bronze_uio.toDF(*[
    c.replace(" ", "_").replace(",", "_").replace(".", "_") for c in df_bronze_uio.columns
])

print(df_silver_uio.columns)

df_silver_uio = df_silver_uio.withColumn("Units", F.regexp_replace("Units", ",", "").cast("int"))
df_silver_uio.show(10)

df_silver_uio.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"`{catalog_name}`.silver.slv_uio")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Supersede Parts

# COMMAND ----------

df_bronze_supersession = spark.table(f'`{catalog_name}`.bronze.brz_supersession')
df_bronze_supersession.show(10)

# rename only the columns that still contain spaces/commas/dots — already-clean names stay untouched
df_silver_supersession = df_bronze_supersession.toDF(*[
    c.replace(" ", "_").replace(",", "_").replace(".", "_") for c in df_bronze_supersession.columns
])

print(df_silver_supersession.columns)

df_silver_supersession.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"`{catalog_name}`.silver.slv_supersession")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spare Parts Orders

# COMMAND ----------

df_bronze_spareparts_orders = spark.table(f'`{catalog_name}`.bronze.brz_spareparts_orders')
df_bronze_spareparts_orders.show(10)

# rename only the columns that still contain spaces/commas/dots — already-clean names stay untouched
df_bronze_spareparts_orders = df_bronze_spareparts_orders.toDF(*[
    c.replace(" ", "_").replace(",", "_").replace(".", "_") for c in df_bronze_spareparts_orders.columns
])

print(df_bronze_spareparts_orders.columns)

# cast the date columns to Date type (source format dd/MM/yyyy; column names were sanitized in the previous cell)
df_silver_spareparts_orders = (df_bronze_spareparts_orders
    .withColumn("Document_Date", F.to_date("Document_Date", "dd/MM/yyyy"))
    .withColumn("Created_On", F.to_date("Created_On", "dd/MM/yyyy"))
    .withColumn("Pricing_Date", F.to_date("Pricing_Date", "dd/MM/yyyy"))
    .withColumn("Delivery_Date", F.to_date("Delivery_Date", "dd/MM/yyyy"))
    .withColumn("Goods_Issue_Date", F.to_date("Goods_Issue_Date", "dd/MM/yyyy"))
    .withColumn("Material_Availability_Date", F.to_date("Material_Availability_Date", "dd/MM/yyyy"))
    .withColumn("Installation_Date", F.to_date("Installation_Date", "dd/MM/yyyy"))
    .withColumn("Acceptance_Date", F.to_date("Acceptance_Date", "dd/MM/yyyy"))
    .withColumn("Contract_Start_Date", F.to_date("Contract_Start_Date", "dd/MM/yyyy"))
    .withColumn("Contract_End_Date", F.to_date("Contract_End_Date", "dd/MM/yyyy"))
    .withColumn("Req_Cancellat_Date", F.to_date("Req_Cancellat_Date", "dd/MM/yyyy"))
    .withColumn("Date_of_Canc_Doc_", F.to_date("Date_of_Canc_Doc_", "dd/MM/yyyy"))
    .withColumn("Dismantling_Date", F.to_date("Dismantling_Date", "dd/MM/yyyy"))
    .withColumn("Action_Date", F.to_date("Action_Date", "dd/MM/yyyy"))
)

#df_silver_spareparts_orders.show(10)

# quantities arrive as formatted strings (thousands separators like "1,000", some decimal-formatted like "1,000.00" / "0.000") — strip commas, cast via double to int
df_silver_spareparts_orders = (df_silver_spareparts_orders
    .withColumn("Order_Quantity_(Item)", F.regexp_replace("Order_Quantity_(Item)", ",", "").cast("double").cast("int"))
    .withColumn("Order_Quantity_(Schedule_Line)", F.regexp_replace("Order_Quantity_(Schedule_Line)", ",", "").cast("double").cast("int"))
    .withColumn("Confirmed_Quantity_(Schedule_Line)", F.regexp_replace("Confirmed_Quantity_(Schedule_Line)", ",", "").cast("double").cast("int"))
    .withColumn("Rounded_Quantity_(Schedule_Line)", F.regexp_replace("Rounded_Quantity_(Schedule_Line)", ",", "").cast("double").cast("int"))
    .withColumn("AF_Open_Confirmed_quantity", F.regexp_replace("AF_Open_Confirmed_quantity", ",", "").cast("double").cast("int"))
    .withColumn("Af_Open_Confirmed_Value", F.regexp_replace("Af_Open_Confirmed_Value", ",", "").cast("double").cast("int"))
    .withColumn("AF_Open_Order_Quantity", F.regexp_replace("AF_Open_Order_Quantity", ",", "").cast("double").cast("int"))
)

#df_silver_spareparts_orders.show(10)

df_silver_spareparts_orders = (df_silver_spareparts_orders
    .withColumn("Net_Value_(Item)", F.regexp_replace("Net_Value_(Item)", ",", "").cast("double"))
    .withColumn("Net_Value_(Header)", F.regexp_replace("Net_Value_(Header)", ",", "").cast("double"))
    .withColumn("Net_Price", F.regexp_replace("Net_Price", ",", "").cast("double"))
)

#df_silver_spareparts_orders.show(10)

# Time is H:MM:SS — keep only the time-of-day as a normalized HH:mm:ss string (Spark has no TIME type)
df_silver_spareparts_orders = df_silver_spareparts_orders.withColumn(
    "Time", F.date_format(F.to_timestamp("Time", "H:mm:ss"), "HH:mm:ss")
)

df_silver_spareparts_orders.show(10)

# Delta rejects parentheses in column names — strip the ones left by the earlier sanitization
df_silver_spareparts_orders = df_silver_spareparts_orders.toDF(*[
    c.replace("(", "").replace(")", "") for c in df_silver_spareparts_orders.columns
])

df_silver_spareparts_orders.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"`{catalog_name}`.silver.slv_spareparts_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spare Parts Sales

# COMMAND ----------

df_bronze_spareparts_sales = spark.table(f'`{catalog_name}`.bronze.brz_spareparts_sales')
df_bronze_spareparts_sales.show(10)

# rename only the columns that still contain spaces/commas/dots — already-clean names stay untouched
df_bronze_spareparts_sales = df_bronze_spareparts_sales.toDF(*[
    c.replace(" ", "_").replace(",", "_").replace(".", "_") for c in df_bronze_spareparts_sales.columns
])

#print(df_bronze_spareparts_sales.columns)

# cast the date columns to Date type (source format dd.MM.yyyy; column names were sanitized in the previous cell; this table has no Time column)
df_silver_spareparts_sales = (df_bronze_spareparts_sales
    .withColumn("Billing_Date", F.to_date("Billing_Date", "dd.MM.yyyy"))
    .withColumn("Date", F.to_date("Date", "dd.MM.yyyy"))
)

#df_silver_spareparts_sales.show(10)

# SlsVolQty contains comma-formatted values (e.g. "1,000.00") — strip commas before casting
df_silver_spareparts_sales = df_silver_spareparts_sales.withColumn("SlsVolQty", F.regexp_replace("SlsVolQty", ",", "").cast("double").cast("int"))

#df_silver_spareparts_sales.show(10)

# money columns use SAP accounting format — comma thousands ("7,010.00"), negatives wrapped in
# parentheses ("(57,267.00)"), and stray spaces ("- 0") — normalize all of that before casting to double
def to_money(c):
    v = F.regexp_replace(c, "[ ,]", "")  # drop thousands separators and stray spaces
    return (F.when(v.rlike(r"^\(.*\)$"), F.concat(F.lit("-"), F.regexp_replace(v, r"[()]", "")))
             .otherwise(v)
             .cast("double"))

df_silver_spareparts_sales = (df_silver_spareparts_sales
    .withColumn("Sales_Pric", to_money("Sales_Pric"))
    .withColumn("Surcharge", to_money("Surcharge"))
    .withColumn("Discount", to_money("Discount"))
    .withColumn("Net_Sales", to_money("Net_Sales"))
    .withColumn("Tax_Amount", to_money("Tax_Amount"))
    .withColumn("Revenue_Re", to_money("Revenue_Re"))
    .withColumn("Cost_Reduc", to_money("Cost_Reduc"))
    .withColumn("Cost", to_money("Cost"))
    .withColumn("Net_Sales_1", to_money("Net_Sales_1"))
)

#df_silver_spareparts_sales.show(10)

df_silver_spareparts_sales.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"`{catalog_name}`.silver.slv_spareparts_sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dealers

# COMMAND ----------

# DBTITLE 1,Cell 13
df_bronze_dealers = spark.table(f'`{catalog_name}`.bronze.brz_dealers')
df_bronze_dealers.show(10)

# build the silver frame from bronze — BG holds comma-thousands text ("700,000") with some
# stray-space values ("- 0"), so strip commas and spaces before the double cast
df_silver_dealers = df_bronze_dealers.withColumn("BG", F.regexp_replace("BG", "[ ,]", "").cast("double"))

# rename columns that still contain spaces/commas/dots/parens — Delta rejects those characters
# in column names (' ,;{}()\n\t=') without column mapping
df_silver_dealers = df_silver_dealers.toDF(*[
    c.replace(" ", "_").replace(",", "_").replace(".", "_").replace("(", "").replace(")", "")
    for c in df_silver_dealers.columns
])

df_silver_dealers.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"`{catalog_name}`.silver.slv_dealers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Current Stock

# COMMAND ----------

df_bronze_current_stock = spark.table(f'`{catalog_name}`.bronze.brz_current_stock')
df_bronze_current_stock.show(10)

# rename only the columns that still contain spaces/commas/dots — already-clean names stay untouched
df_bronze_current_stock = df_bronze_current_stock.toDF(*[
    c.replace(" ", "_").replace(",", "_").replace(".", "_") for c in df_bronze_current_stock.columns
])

print(df_bronze_current_stock.columns)

df_silver_current_stock = df_bronze_current_stock.withColumn("Unrestricted", F.regexp_replace("Unrestricted", "[ ,]", "").cast("double").cast("int"))
#df_silver_current_stock.show(10)

df_silver_current_stock.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"`{catalog_name}`.silver.slv_current_stock")

# COMMAND ----------

# MAGIC %md
# MAGIC ### On Orders

# COMMAND ----------

df_bronze_on_orders = spark.table(f'`{catalog_name}`.bronze.brz_on_orders')
df_bronze_on_orders.show(10)

# rename only the columns that still contain spaces/commas/dots — already-clean names stay untouched
df_bronze_bronze_on_orders = df_bronze_on_orders.toDF(*[
    c.replace(" ", "_").replace(",", "_").replace(".", "_") for c in df_bronze_on_orders.columns
])

print(df_bronze_on_orders.columns)

df_silver_on_orders = (df_bronze_on_orders
    .withColumn("Sep 2026", F.regexp_replace("Sep 2026", "[ ,]", "").cast("double").cast("int"))
    .withColumn("Oct 2026", F.regexp_replace("Oct 2026", "[ ,]", "").cast("double").cast("int"))
    .withColumn("Nov 2026", F.regexp_replace("Nov 2026", "[ ,]", "").cast("double").cast("int"))
    .withColumn("Dec 2026", F.regexp_replace("Dec 2026", "[ ,]", "").cast("double").cast("int"))
)

#df_silver_on_orders.show(10)

# Delta rejects spaces/commas/dots in column names — sanitize before writing
# (the upstream rename landed on a different variable, so the frame reaching here was never renamed)
df_silver_on_orders = df_silver_on_orders.toDF(*[
    c.replace(" ", "_").replace(",", "_").replace(".", "_") for c in df_silver_on_orders.columns
])

print(df_silver_on_orders.columns)

df_silver_on_orders.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"`{catalog_name}`.silver.slv_on_orders")

# COMMAND ----------

# df = df_bronze_unit_sales.select("Material").distinct()

# model_list = {
#               "RAY ZR 125 STREET RALLY" , "Ray ZR Disc" , "RAY ZR 125 DISC" , "Ray ZR Street Rally",
#               "MT15 V2",
#               "FZS-FI V4(STD)" , "FZS-FI V4 (STD)" ,  "FZS-FI V4" , "FZ-S-FI NEW" , "FZS-DLX-149" , "FZ-S DLX 149",
#               "FZ FI V2",
#               "YZF-R15 V4", 
#               "FASINO S 125",
#               "XSR-155-DD10F" , "XSR-155-DD10G" , "XSR-155-DD10A",
#               "NMAX-10A-155"
# }

# import re

# # one pattern from model_list — longest names first so the most specific model wins
# pattern = "|".join(re.escape(m) for m in sorted(model_list, key=len, reverse=True))

# df_classified = (df
#     .withColumn("model", F.regexp_extract("Material", f"(?i)({pattern})", 1))
#     .withColumn("color", F.regexp_replace("Material", f"(?i){pattern}", ""))
#     # strip the symbols ( ) - . from the color values, then tidy up spacing
#     .withColumn("color", F.trim(F.regexp_replace(F.regexp_replace("color", "[()\\-.]", ""), "\\s+", " ")))
# )

# display(df_classified)

# model_anomalies = {
#     "FZS-FI V4(STD)" : "FZ-S FI V4 (STD)",
#     "FZS-FI V4 (STD)" : "FZ-S FI V4 (STD)",
#     "FZ-S-FI NEW" : "FZ-S FI NEW",
#     "FZS-DLX-149" : "FZ-S DLX",
#     "FZS-FI V4" : "FZ-S FI V4",
#     "RAY ZR 125 STREET RALLY" : "RAY ZR 125 Street Rally",
#     "RAY ZR 125 DISC" : "RAY ZR 125 Disc",
#     "MT15 V2" : "MT-15 V2",
#     "NMAX-10A-155" : "NMAX 155",
#     "XSR-155-DD10F" : "XSR 155",
#     "XSR-155-DD10G" : "XSR 155",
#     "XSR-155-DD10A" : "XSR 155"
# }

# # replace raw model names with their standardized names from model_anomalies
# df_classified = df_classified.replace(model_anomalies, subset=["model"])

# display(df_classified)

# color_anomalies = {
#     "DBNM8" : "DARK BLUISH GRAY METALLIC 8",
#     "DPBMY" : "DEEP PURPLISH BLUE METALLIC Y",
#     "MLNM4" : "MAT LIGHT GRAY METALLIC 4",
#     "MDNM9" : "MAT DARK GRAY METALLIC 9",
#     "MBL2" : "MAT BLACK 2",
#     "SM12" : "BLACK METALLIC 12",
#     "BMC" : "BLUE METALLIC C",
#     "MGM3" : "GRAYISH GREEN METALLIC 3",
#     "WMC" : "WHITE METALLIC C"
# }

# # replace raw color names with their standardized names from color_anomalies
# df_classified = df_classified.replace(color_anomalies, subset=["color"])

# display(df_classified)
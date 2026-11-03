# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, FloatType, DecimalType, IntegerType,DateType
import pyspark.sql.functions as F

#!pip install openpyxl

catalog_name = 'yamaha-catalog'

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Unit Sales

# COMMAND ----------

#define schema
unit_sales_schema = StructType([
  StructField('Sales Organization', StringType(), True),
  StructField('Distribution Channel', StringType(), True),
  StructField('Sales Office', StringType(), True),
  StructField('Sales Employee', StringType(), True),
  StructField('Payer', StringType(), True),
  StructField('Material', StringType(), True),
  StructField('Billing Date', StringType(), True),
  StructField('Billing Document', StringType(), True),
  StructField('Item', StringType(), True),
  StructField('Date', StringType(), True),
  StructField('SlsVolQty',StringType(), True),
  StructField('SlsVolQty2', StringType(), True),
  StructField('Ad Payment', StringType(), True),
  StructField('Ad Payment2', StringType(), True),
  StructField('Sales Price', StringType(), True),
  StructField('Sales Price2', StringType(), True),
  StructField('Surcharge', StringType(), True),
  StructField('Surcharge2', StringType(), True),
  StructField('Discount', StringType(), True),
  StructField('Discount2', StringType(), True),
  StructField('Net Sales', StringType(), True),
  StructField('Net Sales3', StringType(), True),
  StructField('Tax Amount', StringType(), True),
  StructField('Tax Amount2', StringType(), True),
  StructField('Tax Code', StringType(), True),
  StructField('Revenue Re', StringType(), True),
  StructField('Revenue Re2', StringType(), True),
  StructField('Cost Reduc', StringType(), True),
  StructField('Cost Reduc2', StringType(), True),
  StructField('Batch', StringType(), True),
  StructField('Bill. Type', StringType(), True),
  StructField('Cost', StringType(), True),
  StructField('Cost2', StringType(), True),
  StructField('Cust. Clas', StringType(), True),
  StructField('CustGrpOrd', StringType(), True),
  StructField('Division', StringType(), True),
  StructField('Matl Group', StringType(), True),
  StructField('Net Sales4', StringType(), True),
  StructField('Net Sales2', StringType(), True),
  StructField('Prod.Hier.', StringType(), True),
  StructField('Sales Grp', StringType(), True),
  StructField('VIN', StringType(), True),
  StructField('Year', StringType(), True),
  StructField('Month', StringType(), True),
  StructField('Dealer code', StringType(), True),
  StructField('Dealer Name', StringType(), True),
  StructField('Dealer Name 2', StringType(), True),
  StructField('Province', StringType(), True),
  StructField('RM', StringType(), True),
  StructField('ASE', StringType(), True),
  StructField('District', StringType(), True),
  StructField('Model', StringType(), True),
  StructField('Model Name', StringType(), True),
  StructField('Color', StringType(), True),
  StructField('Type', StringType(), True),
  StructField('Customer Id', StringType(), True),
  StructField('Age At Purchase', StringType(), True),
  StructField('Age Today', StringType(), True)
])


source_file = '/Volumes/yamaha-catalog/source_data/raw/unit sales/*.xlsx'

df = spark.read.option("headerRows", 1).schema(unit_sales_schema).excel(source_file).withColumn("_source_file", F.col("_metadata.file_path")).withColumn("ingested_at", F.current_timestamp())

display(df.limit(10))


(df.write.format("delta")
   .mode("overwrite")
   .option("delta.columnMapping.mode", "name")
   .option("delta.minReaderVersion", "2")
   .option("delta.minWriterVersion", "5")
   .saveAsTable(f"`{catalog_name}`.bronze.brz_unit_sales"))


# COMMAND ----------

# MAGIC %md
# MAGIC ### UIO

# COMMAND ----------

#define schema
uio_schema = StructType([
  StructField('Model', StringType(), True),
  StructField('Model Name', StringType(), True),
  StructField('Status', StringType(), True),
  StructField('Model Family', StringType(), True),
  StructField('Motorcycle Type', StringType(), True),
  StructField('Segment', StringType(), True),
  StructField('cc', StringType(), True),
  StructField('Color', StringType(), True),
  StructField('Abbreviation', StringType(), False),
  StructField('Color Code', StringType(), False),
  StructField('Color Family', StringType(), True),
  StructField('Units', StringType(), True)
])


source_file = '/Volumes/yamaha-catalog/source_data/raw/uio/*.xlsx'

df = spark.read.option("headerRows", 1).schema(uio_schema).excel(source_file).withColumn("_source_file", F.col("_metadata.file_path")).withColumn("ingested_at", F.current_timestamp())

display(df.limit(10))


(df.write.format("delta")
  .mode("overwrite")
  .option("mergeSchema", "true")
  .option("delta.columnMapping.mode", "name")
  .option("delta.minReaderVersion", "2")
  .option("delta.minWriterVersion", "5")
  .saveAsTable(f"`{catalog_name}`.bronze.brz_uio"))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Supersede Parts

# COMMAND ----------

#define schema
supersede_schema = StructType([
  StructField('Material', StringType(), True),
  StructField('Latest SS', StringType(), True),
  StructField('Material description', StringType(), True),
  StructField('Material Type', StringType(), True),
  StructField('Material Group', StringType(), True),
  StructField('Brand', StringType(), True),
  StructField('1st Supersede', StringType(), True),
  StructField('2nd Supersede', StringType(), True),
  StructField('3rd Supersede', StringType(), True),
  StructField('4th Supersede', StringType(), False),
  StructField('5th Supersede', StringType(), False),
  StructField('6th Supersede', StringType(), True),
  StructField('7th Supersede', StringType(), True),
  StructField('8th Supersede', StringType(), True),
  StructField('9th Supersede', StringType(), True),
  StructField('10th Supersede', StringType(), True)
])


source_file = '/Volumes/yamaha-catalog/source_data/raw/supersession/*.xlsx'

df = spark.read.option("headerRows", 1).schema(supersede_schema).excel(source_file).withColumn("_source_file", F.col("_metadata.file_path")).withColumn("ingested_at", F.current_timestamp())

display(df.limit(10))


(df.write.format("delta")
  .mode("overwrite")
  .option("mergeSchema", "true")
  .option("delta.columnMapping.mode", "name")
  .option("delta.minReaderVersion", "2")
  .option("delta.minWriterVersion", "5")
  .saveAsTable(f"`{catalog_name}`.bronze.brz_supersession"))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Spare Parts Orders

# COMMAND ----------

#define schema
spareparts_orders_schema = StructType([
  StructField('Customer Reference', StringType(), True),
  StructField('Document Date', StringType(), True),
  StructField('Sales Document Type', StringType(), True),
  StructField('Sales Document', StringType(), True),
  StructField('Sales Document Item', StringType(), True),
  StructField('Sold-to Party', StringType(), True),
  StructField('Material', StringType(), True),
  StructField('Order Quantity (Item)', StringType(), True),
  StructField('Sales Unit', StringType(), True),
  StructField('Net Value (Item)', StringType(), True),
  StructField('Document Currency', StringType(), True),
  StructField('Order Reason', StringType(), True),
  StructField('Customer Reference (Header)', StringType(), True),
  StructField('Created By', StringType(), True),
  StructField('Created On', StringType(), True),
  StructField('Time', StringType(), True),
  StructField('Billing Block', StringType(), True), #NULL
  StructField('Sales Document Description', StringType(), True), #NULL
  StructField('Exchange Rate Type', StringType(), True), #NULL
  StructField('Delivery Block', StringType(), True), #NULL
  StructField('Net Value (Header)', StringType(), True),
  StructField('Division', StringType(), True),
  StructField('SD Document Category', StringType(), True),
  StructField('Sales Office', StringType(), True),
  StructField('Sales Group', StringType(), True),
  StructField('Sales Organization', StringType(), True),
  StructField('Distribution Channel', StringType(), True),
  StructField('Overall Status', StringType(), True),
  StructField('Overall Delivery Status (All Items)', StringType(), True),
  StructField('Sold-To Party Name', StringType(), True),
  StructField('Reason for Rejection', StringType(), True), #NULL
  StructField('Material Description', StringType(), True),
  StructField('Batch', StringType(), True), #NULL
  StructField('Confirmed Quantity (Item)', StringType(), True),
  StructField('Pricing Unit', StringType(), True),
  StructField('Pricing Unit (per)', StringType(), True),
  StructField('Storage Location', StringType(), True), #NULL
  StructField('Base Unit of Measure', StringType(), True),
  StructField('Net Price', StringType(), True),
  StructField('Returns', StringType(), True),
  StructField('Shipping Point/Receiving Pt', StringType(), True),
  StructField('Plant', StringType(), True),
  StructField('Target Quantity', StringType(), True),
  StructField('Overall Status Item', StringType(), True),
  StructField('Overall Delivery Status (Item)', StringType(), True),
  StructField('Exchange Rate', StringType(), True),
  StructField('Pricing Date', StringType(), True),
  StructField('Personnel Partner Function', StringType(), True), #NULL
  StructField('Personnel Number', StringType(), True),
  StructField('Partner Function', StringType(), True), #NULL
  StructField('Address Number', StringType(), True), #NULL
  StructField('Partner', StringType(), True), #NULL
  StructField('Schedule Line Number', StringType(), True),
  StructField('Delivery Date', StringType(), True),
  StructField('Order Quantity (Schedule Line)', StringType(), True),
  StructField('Confirmed Quantity (Schedule Line)', StringType(), True),
  StructField('Rounded Quantity (Schedule Line)', StringType(), True),
  StructField('Goods Issue Date', StringType(), True),
  StructField('Material Availability Date', StringType(), True),
  StructField('Sched. Line Category', StringType(), True),
  StructField('Contract Val.Period', StringType(), True),
  StructField('Unit Validity Period', StringType(), True), #NULL
  StructField('Val.Period.Category', StringType(), True), #NULL
  StructField('Installation Date', StringType(), True), #NULL
  StructField('Acceptance Date', StringType(), True), #NULL
  StructField('Contract Signed', StringType(), True), #NULL
  StructField('Contract Start Date', StringType(), True), #NULL
  StructField('Contract End Date', StringType(), True), #NULL
  StructField('Cancellation Proced.', StringType(), True), #NULL
  StructField('Action', StringType(), True), #NULL
  StructField('Receipt of Canc.', StringType(), True), #NULL
  StructField('Req.Cancellat.Date', StringType(), True), #NULL
  StructField('Cancellation Party', StringType(), True), #NULL
  StructField('Reason for Cancel.', StringType(), True), #NULL
  StructField('Cancel.Doc.for Part.', StringType(), True), #NULL
  StructField('Date of Canc.Doc.', StringType(), True), #NULL
  StructField('Dismantling Date', StringType(), True), #NULL
  StructField('Action Date', StringType(), True), #NULL
  StructField('Delivery Block Description', StringType(), True), #NULL
  StructField('Billing Block Description', StringType(), True), #NULL
  StructField('Order Reason Description', StringType(), True),
  StructField('Rejection Reason Description', StringType(), True), #NULL
  StructField('Personnel Name', StringType(), True), #NULL
  StructField('Overall Status Description', StringType(), True),
  StructField('Overall Delivery Status Description', StringType(), True),
  StructField('Overall Status Item Description', StringType(), True),
  StructField('Overall Delivery Status Item Description', StringType(), True),
  StructField('Sales Organization Name', StringType(), True),
  StructField('Distribution Channel Name', StringType(), True),
  StructField('Division Name', StringType(), True),
  StructField('Sales Document Type Description', StringType(), True),
  StructField('Address Blocked (Sold-To)', StringType(), True), #NULL
  StructField('Requirement Segment', StringType(), True), #NULL
  StructField('Address Blocked (Partner)', StringType(), True), #NULL
  StructField('Employee ID', StringType(), True), #NULL
  StructField('Action Description', StringType(), True), #NULL
  StructField('Item Category', StringType(), True),
  StructField('AF Open Confirmed quantity', StringType(), True),
  StructField('Af Open Confirmed Value', StringType(), True),
  StructField('Complete name', StringType(), True),
  StructField('AF Open Order Quantity', StringType(), True)
])


source_file = '/Volumes/yamaha-catalog/source_data/raw/spare-parts orders/*.xlsx'

df = spark.read.option("headerRows", 1).schema(spareparts_orders_schema).excel(source_file).withColumn("_source_file", F.col("_metadata.file_path")).withColumn("ingested_at", F.current_timestamp())

display(df.limit(10))


(df.write.format("delta")
  .mode("overwrite")
  .option("mergeSchema", "true")
  .option("delta.columnMapping.mode", "name")
  .option("delta.minReaderVersion", "2")
  .option("delta.minWriterVersion", "5")
  .saveAsTable(f"`{catalog_name}`.bronze.brz_spareparts_orders"))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Spare Parts Sales

# COMMAND ----------

# DBTITLE 1,Spare-parts sales bronze load
#define schema
spareparts_sales_schema = StructType([
  StructField('Sales Organization', StringType(), True),
  StructField('Distribution Channel', StringType(), True),
  StructField('Sales Office', StringType(), True),
  StructField('Sales Employee', StringType(), True),
  StructField('Payer', StringType(), True),
  StructField('Material', StringType(), True),
  StructField('Billing Date', StringType(), True),
  StructField('Billing Document', StringType(), True),
  StructField('Item', StringType(), True),
  StructField('Date', StringType(), True),
  StructField('SlsVolQty', StringType(), True),
  StructField('SlsVolQty2', StringType(), True),
  StructField('Ad Payment', StringType(), True),
  StructField('Ad Payment2', StringType(), True),
  StructField('Sales Pric', StringType(), True),
  StructField('Sales Pric2', StringType(), True),
  StructField('Surcharge', StringType(), True),
  StructField('Surcharge2', StringType(), True),
  StructField('Discount', StringType(), True),
  StructField('Discount2', StringType(), True),
  StructField('Net Sales', StringType(), True),
  StructField('Net Sales2', StringType(), True),
  StructField('Tax Amount', StringType(), True),
  StructField('Tax Amount2', StringType(), True),
  StructField('Tax Code', StringType(), True),
  StructField('Revenue Re', StringType(), True),
  StructField('Revenue Re2', StringType(), True),
  StructField('Cost Reduc', StringType(), True),
  StructField('Cost Reduc2', StringType(), True),
  StructField('Batch', StringType(), True), #NULL
  StructField('Bill. Type', StringType(), True),
  StructField('Cost', StringType(), True),
  StructField('Cost2', StringType(), True),
  StructField('Cust. Clas', StringType(), True),
  StructField('CustGrpOrd', StringType(), True),
  StructField('Division', StringType(), True),
  StructField('Matl Group', StringType(), True),
  StructField('Net Sales_1', StringType(), True),
  StructField('Net Sales_2', StringType(), True),
  StructField('Prod.Hier.', StringType(), True),
  StructField('Sales Grp', StringType(), True)
])


source_file = '/Volumes/yamaha-catalog/source_data/raw/spare-parts sales/*.xlsx'

df = spark.read.option("headerRows", 1).schema(spareparts_sales_schema).excel(source_file).withColumn("_source_file", F.col("_metadata.file_path")).withColumn("ingested_at", F.current_timestamp())

display(df.limit(10))


(df.write.format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .option("delta.columnMapping.mode", "name")
  .option("delta.minReaderVersion", "2")
  .option("delta.minWriterVersion", "5")
  .saveAsTable(f"`{catalog_name}`.bronze.brz_spareparts_sales"))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Dealers

# COMMAND ----------

#define schema
dealers_schema = StructType([
  StructField('Dealer Code', StringType(), True),
  StructField('Dealer Name', StringType(), True),
  StructField('Province', StringType(), True),
  StructField('District', StringType(), True),
  StructField('ASE', StringType(), True),
  StructField('RM', StringType(), True),
  StructField('Admin', StringType(), True),
  StructField('BG', StringType(), True),
  StructField('Credit Limit', StringType(), True),
  StructField('Cash / Credit Status', StringType(), True),
  StructField('Type', StringType(), True),
  StructField('Status (Active/ Inactive)', StringType(), True)
])


source_file = '/Volumes/yamaha-catalog/source_data/raw/dealers/*.xlsx'

df = spark.read.option("headerRows", 1).schema(dealers_schema).excel(source_file).withColumn("_source_file", F.col("_metadata.file_path")).withColumn("ingested_at", F.current_timestamp())

display(df.limit(10))


(df.write.format("delta")
  .mode("overwrite")
  .option("mergeSchema", "true")
  .option("delta.columnMapping.mode", "name")
  .option("delta.minReaderVersion", "2")
  .option("delta.minWriterVersion", "5")
  .saveAsTable(f"`{catalog_name}`.bronze.brz_dealers"))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Current Stock

# COMMAND ----------

#define schema
current_stock_schema = StructType([
  StructField('Name 1', StringType(), True),
  StructField('Plant', StringType(), True),
  StructField('Material Group', StringType(), True),
  StructField('Material', StringType(), True),
  StructField('Material Description', StringType(), True),
  StructField('Storage Location', StringType(), True),
  StructField('Descr. of Storage Loc.', StringType(), True),
  StructField('Unrestricted', StringType(), True)
])


source_file = '/Volumes/yamaha-catalog/source_data/raw/current stock/*.xlsx'

df = spark.read.option("headerRows", 1).schema(current_stock_schema).excel(source_file).withColumn("_source_file", F.col("_metadata.file_path")).withColumn("ingested_at", F.current_timestamp())

display(df.limit(10))


(df.write.format("delta")
  .mode("overwrite")
  .option("mergeSchema", "true")
  .option("delta.columnMapping.mode", "name")
  .option("delta.minReaderVersion", "2")
  .option("delta.minWriterVersion", "5")
  .saveAsTable(f"`{catalog_name}`.bronze.brz_current_stock"))


# COMMAND ----------

# MAGIC %md
# MAGIC ### On Orders

# COMMAND ----------

#define schema
on_orders_schema = StructType([
  StructField('Material', StringType(), True),
  StructField('Description', StringType(), True),
  StructField('Superseded', StringType(), True),
  StructField('Latest SS', StringType(), True),
  StructField('Sep 2026', StringType(), True),
  StructField('Oct 2026', StringType(), True),
  StructField('Nov 2026', StringType(), True),
  StructField('Dec 2026', StringType(), True)
])


source_file = '/Volumes/yamaha-catalog/source_data/raw/on orders/*.xlsx'

df = spark.read.option("headerRows", 1).schema(on_orders_schema).excel(source_file).withColumn("_source_file", F.col("_metadata.file_path")).withColumn("ingested_at", F.current_timestamp())

display(df.limit(10))


(df.write.format("delta")
  .mode("overwrite")
  .option("mergeSchema", "true")
  .option("delta.columnMapping.mode", "name")
  .option("delta.minReaderVersion", "2")
  .option("delta.minWriterVersion", "5")
  .saveAsTable(f"`{catalog_name}`.bronze.brz_on_orders"))


# COMMAND ----------

# Load every other Excel file in raw/ into bronze, one table per folder (brz_<folder>)
# Same steps as unit sales: pandas read -> cast -> Spark DataFrame -> metadata -> Delta,
# but the schema is inferred and column names are cleaned to snake_case
#import os, re
#import pandas as pd
#import pyspark.sql.functions as F

#catalog_name = 'yamaha-catalog'
#raw_root = '/Volumes/yamaha-catalog/source_data/raw'
#skip = {'unit sales'}  # already loaded in the Unit Sales cell

#def clean(name, i=0):
#    n = re.sub(r'[^0-9a-zA-Z]+', '_', str(name)).strip('_').lower()
#    return n or f'col_{i}'

#def load_to_bronze(path, table, mode):
#    pdf = pd.read_excel(path)

    # snake_case column names, de-duplicated
#    cols = []
#    for i, c in enumerate(pdf.columns):
#        n = clean(c, i)
#        while n in cols:
#            n += '_dup'
#        cols.append(n)
#    pdf.columns = cols

    # text, mixed number/text and time-only columns -> string (nulls stay null)
#    for c in pdf.columns:
#        if pdf[c].dtype == object:
#            pdf[c] = pdf[c].astype('string')

    # metadata columns
#    df = (spark.createDataFrame(pdf))

#    w = df.write.format('delta').mode(mode)
#    w = w.option('overwriteSchema', 'true') if mode == 'overwrite' else w.option('mergeSchema', 'true')
#    w.saveAsTable(f'`{catalog_name}`.bronze.{table}')
#    return pdf.shape

#for folder in sorted(os.listdir(raw_root)):
#    if folder in skip:
#       continue
#    table = 'brz_' + clean(folder)
#    files = [f for f in sorted(os.listdir(os.path.join(raw_root, folder))) if f.endswith('.xlsx')]
#    for k, f in enumerate(files):
#        # first file replaces the table, the rest of the folder appends (e.g. multiple UIO files)
#        mode = 'overwrite' if k == 0 else 'append'
#       try:
#            rows, ncols = load_to_bronze(os.path.join(raw_root, folder, f), table, mode)
#            print(f'OK   {table} <- {f}: {rows:,} rows x {ncols} cols ({mode})')
#        except Exception as e:
#            print(f'FAIL {table} <- {f}: {str(e)[:300]}')

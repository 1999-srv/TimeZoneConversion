# TimeZoneConversion
This PySpark script processes sales order data, converts timestamps into multiple European time zones, and partitions the data by year and month. The data is then saved into separate Delta tables based on the year and month.

# Requirements
Apache Spark with PySpark installed.
Sales order data stored in a CSV file with columns such as SalesOrderID, OrderQty, ProductID, and ModifiedDate.
Script Overview
Step 1: Define the Schema
The schema is defined using PySpark's StructType, which ensures that each column has the correct data type, including the ModifiedDate which is of type DateType.


schema = StructType([
    StructField("SalesOrderID", IntegerType(), True),
    StructField("SalesOrderDetailID", IntegerType(), True),
    StructField("CarrierTrackingNumber", StringType(), True),
    ...
    StructField("ModifiedDate", DateType(), True)
])

Step 2: Load the Data
The sales order data is loaded from a CSV file located at /FileStore/tables/Sales_SalesOrderDetail__2_.csv. The file is read with the defined schema to ensure proper column data types.

file_path = '/FileStore/tables/Sales_SalesOrderDetail__2_.csv'
df = spark.read.csv(file_path, header=True, schema=schema)

Step 3: Convert the Date to UTC and European Time Zones
The ModifiedDate column is first converted to a timestamp in UTC. Then, additional columns are created to convert the timestamp into five European time zones using from_utc_timestamp() for daylight saving time handling:

London (GMT/WET)
Paris (CET)
Helsinki (EET)
Moscow (MSK)
Lisbon (WEST)

df = df.withColumn("UTC", to_timestamp(col("ModifiedDate"), "yyyy-MM-dd"))
df = df.withColumn("London_GMT/WET", from_utc_timestamp(col("UTC"), "Europe/London")) \
       .withColumn("Paris_CET", from_utc_timestamp(col("UTC"), "Europe/Paris")) \
       .withColumn("Helsinki_EET", from_utc_timestamp(col("UTC"), "Europe/Helsinki")) \
       .withColumn("Moscow_MSK", from_utc_timestamp(col("UTC"), "Europe/Moscow")) \
       .withColumn("Lisbon_WEST", from_utc_timestamp(col("UTC"), "Europe/Lisbon"))

Step 4: Extract Year and Month
The script extracts the Year and Month from the ModifiedDate column and combines them into a new YearMonth column for easy partitioning.

df = df.withColumn("Year", year(col("ModifiedDate"))) \
       .withColumn("Month", month(col("ModifiedDate"))) \
       .withColumn("YearMonth", concat(col("Year"), lit("_"), col("Month")))
Step 5: Save Data by Year and Month
The data is partitioned by YearMonth, and each partition is saved as a separate Delta table. Each table is named based on the YearMonth, and the schema is automatically merged if necessary.

for ym in year_months:
    df_ym = df.filter(col("YearMonth") == ym)
    df_ym.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable(f"default.sales_data_{ym}", header=True)

# Output
The script outputs the sales order data into separate Delta tables for each YearMonth. Each table is saved with the format sales_data_<Year>_<Month> (e.g., sales_data_2024_08).

# Usage Instructions
Place the sales data CSV file at the specified path: /FileStore/tables/Sales_SalesOrderDetail__2_.csv.
Run the script in a PySpark-enabled environment.
The script will convert the timestamps, partition the data by Year and Month, and save the results into Delta tables.

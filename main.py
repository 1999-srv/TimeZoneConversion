from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, DateType
from pyspark.sql.functions import col, to_timestamp, from_utc_timestamp, year, month, concat, lit


# Initialize Spark session
spark = SparkSession.builder.appName("SalesOrderDetailProcessing").getOrCreate()

# Define the schema
schema = StructType([
    StructField("SalesOrderID", IntegerType(), True),
    StructField("SalesOrderDetailID", IntegerType(), True),
    StructField("CarrierTrackingNumber", StringType(), True),
    StructField("OrderQty", IntegerType(), True),
    StructField("ProductID", IntegerType(), True),
    StructField("SpecialOfferID", IntegerType(), True),
    StructField("UnitPrice", DecimalType(10, 2), True),
    StructField("UnitPriceDiscount", DecimalType(10, 2), True),
    StructField("LineTotal", DecimalType(20, 2), True),
    StructField("rowguid", StringType(), True),
    StructField("ModifiedDate", DateType(), True)
])

# Load the CSV file with the defined schema
file_path = '/FileStore/tables/Sales_SalesOrderDetail__2_.csv'
df = spark.read.csv(file_path, header=True, schema=schema)

# Convert ModifiedDate into timestamp
df = df.withColumn("UTC", to_timestamp(col("ModifiedDate"), "yyyy-MM-dd"))

# Add columns for 5 European time zones with DST handling
df = df.withColumn("London_GMT/WET", from_utc_timestamp(col("UTC"), "Europe/London")) \
       .withColumn("Paris_CET", from_utc_timestamp(col("UTC"), "Europe/Paris")) \
       .withColumn("Helsinki_EET", from_utc_timestamp(col("UTC"), "Europe/Helsinki")) \
       .withColumn("Moscow_MSK", from_utc_timestamp(col("UTC"), "Europe/Moscow")) \
       .withColumn("Lisbon_WEST", from_utc_timestamp(col("UTC"), "Europe/Lisbon"))

# Extract year and month from ModifiedDate
df = df.withColumn("Year", year(col("ModifiedDate"))) \
       .withColumn("Month", month(col("ModifiedDate")))

# Combine Year and Month into a single column for easier partitioning
df = df.withColumn("YearMonth", concat(col("Year"), lit("_"), col("Month")))

# Show the resulting DataFrame with year and month
df.select("ModifiedDate", "Year", "Month", "YearMonth", "UTC", "London_GMT/WET", "Paris_CET", "Helsinki_EET", "Moscow_MSK", "Lisbon_WEST").show()

# Divide the data by year and month and save into different files based on Year and Month
year_months = df.select("YearMonth").distinct().rdd.flatMap(lambda x: x).collect()

for ym in year_months:
    df_ym = df.filter(col("YearMonth") == ym)
    # Save each year and month's data into separate files
    df_ym.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable(f"default.sales_data_{ym}", header=True)

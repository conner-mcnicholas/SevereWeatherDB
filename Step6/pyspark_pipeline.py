from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import *

spark = SparkSession.builder().master("local[*]")
    .appName("PipelinePyspark")
    .getOrCreate()

schema_details = StructType([
    StructField("BEGIN_YEARMONTH", StringType(),True),
    StructField("BEGIN_DAY", StringType(),True),
    StructField("BEGIN_TIME", StringType(),True),
    StructField("END_YEARMONTH", StringType(),True),
    StructField("END_DAY", StringType(),True),
    StructField("END_TIME", StringType(),True),
    StructField("EPISODE_ID", IntegerType(),True),
    StructField("EVENT_ID", IntegerType(),True),
    StructField("STATE", StringType(),True),
    StructField("STATE_FIPS", IntegerType(),True),
    StructField("EVENT_TYPE", StringType(),True),
    StructField("CZ_TYPE", StringType(),True),
    StructField("CZ_FIPS", IntegerType(),True),
    StructField("CZ_NAME", StringType(),True),
    StructField("WFO", StringType(),True),
    StructField("CZ_TIMEZONE", StringType(),True),
    StructField("INJURIES_DIRECT", IntegerType(),True),
    StructField("INJURIES_INDIRECT", IntegerType(),True),
    StructField("DEATHS_DIRECT", IntegerType(),True),
    StructField("DEATHS_INDIRECT", IntegerType(),True),
    StructField("DAMAGE_PROPERTY", StringType(),True),
    StructField("DAMAGE_CROPS", StringType(),True),
    StructField("SOURCE", StringType(),True),
    StructField("MAGNITUDE", DoubleType(),True),
    StructField("MAGNITUDE_TYPE", StringType(),True),
    StructField("FLOOD_CAUSE", StringType(),True),
    StructField("CATEGORY", IntegerType(),True),
    StructField("TOR_F_SCALE", StringType(),True),
    StructField("TOR_LENGTH", DoubleType(),True),
    StructField("TOR_WIDTH", IntegerType(),True),
    StructField("TOR_OTHER_WFO", StringType(),True),
    StructField("TOR_OTHER_CZ_STATE", StringType(),True),
    StructField("TOR_OTHER_CZ_FIPS", IntegerType(),True),
    StructField("TOR_OTHER_CZ_NAME", StringType(),True),
    StructField("BEGIN_RANGE", IntegerType(),True),
    StructField("BEGIN_AZIMUTH", StringType(),True),
    StructField("BEGIN_LOCATION", StringType(),True),
    StructField("END_RANGE", IntegerType(),True),
    StructField("END_AZIMUTH", StringType(),True),
    StructField("END_LOCATION", StringType(),True),
    StructField("BEGIN_LAT", DoubleType(),True),
    StructField("BEGIN_LON", DoubleType(),True),
    StructField("END_LAT", DoubleType(),True),
    StructField("END_LON", DoubleType(),True),
    StructField("EPISODE_NARRATIVE",StringType(),True),
    StructField("EVENT_NARRATIVE",StringType(),True)
])

schema_fatalities = StructType([
    StructField("FATALITY_ID INT", IntegerType(),True),
    StructField("FATALITY_ID", IntegerType(),True),
    StructField("EVENT_ID", IntegerType(),True),
    StructField("FATALITY_TYPE", StringType(),True),
    StructField("FATALITY_DATE", StringType(),True),
    StructField("FATALITY_AGE", IntegerType(),True),
    StructField("FATALITY_SEX", StringType(),True),
    StructField("FATALITY_LOCATION",IntegerType(),True),
    StructField("EVENT_YEARMONTH",StringType(),True)
])

#mnt to azure blob instead of local
df_details = spark.read.format("csv")
    .option("header", True),
    .schema(schema_details)
    .load("/mnt/storm_details_*")

#mnt to azure blob instead of local
df_fatalities = spark.read.format("csv")
    .option("header", True),
    .schema(schema_fatalities)
    .load("/mnt/storm_fatalities_*")

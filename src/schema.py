from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType, IntegerType

# Define schema
def define_schema():
    schema = StructType([
        StructField("id", LongType(), True),
        StructField("sampling_rate", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("location", StructType([
            StructField("id", LongType(), True),
            StructField("latitude", StringType(), True),
            StructField("longitude", StringType(), True),
            StructField("altitude", StringType(), True),
            StructField("country", StringType(), True),
            StructField("exact_location", IntegerType(), True),
            StructField("indoor", IntegerType(), True)
        ])),
        StructField("sensor", StructType([
            StructField("id", LongType(), True),
            StructField("pin", StringType(), True),
            StructField("sensor_type", StructType([
                StructField("id", LongType(), True),
                StructField("name", StringType(), True),
                StructField("manufacturer", StringType(), True)
            ]))
        ])),
        StructField("sensordatavalues", ArrayType(StructType([
            StructField("id", LongType(), True),
            StructField("value", StringType(), True),
            StructField("value_type", StringType(), True)
        ])))
    ])
    return schema
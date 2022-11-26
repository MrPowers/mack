import pytest
import chispa
import pyspark
from delta import *
import datetime
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, BooleanType, DateType
import mack

builder = (
    pyspark.sql.SparkSession.builder.appName("MyApp")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

def test_type_2_scd_generic_upsert():
    # versions = [0, 1]
    # for v in versions:
    #     actual_df = spark.read.format("delta").option("versionAsOf", v).load("out/tables/generated/reference_table_1/delta")
    #     expected_df = spark.read.format("parquet").load(f"out/tables/generated/reference_table_1/expected/v{v}/table_content.parquet")
    #     chispa.assert_df_equality(actual_df, expected_df)

    path = "tmp/delta-upsert-date"
    # // create Delta Lake
    data2 = [
        (1, "A", True, datetime.datetime(2019, 1, 1), None),
        (2, "B", True, datetime.datetime(2019, 1, 1), None),
        (4, "D", True, datetime.datetime(2019, 1, 1), None),
    ]

    schema = StructType([
        StructField("pkey",IntegerType(),True),
        StructField("attr",StringType(),True),
        StructField("cur",BooleanType(),True),
        StructField("effective_date", DateType(), True),
        StructField("end_date", DateType(), True)
    ])

    df = spark.createDataFrame(data=data2,schema=schema)
    df.write.format("delta").save(path)

    # create updates DF
    updatesDF = spark.createDataFrame([
        (3, "C", datetime.datetime(2020, 9, 15)), # new value
        (2, "Z", datetime.datetime(2020, 1, 1)), # value to upsert
    ]).toDF("pkey", "attr", "effective_date")

    # perform upsert
    mack.type_2_scd_generic_upsert(path, updatesDF, "pkey", ["attr"], "cur", "effective_date", "end_date")

    actual_df = spark.read.format("delta").load(path)

    expected_df = spark.createDataFrame([
        (2, "B", False, datetime.datetime(2019, 1, 1), datetime.datetime(2020, 1, 1)),
        (3, "C", True, datetime.datetime(2020, 9, 15), None),
        (2, "Z", True, datetime.datetime(2020, 1, 1), None),
        (4, "D", True, datetime.datetime(2019, 1, 1), None),
        (1, "A", True, datetime.datetime(2019, 1, 1), None),
    ], schema)

    chispa.assert_df_equality(actual_df, expected_df, ignore_row_order=True)

    # val expected = Seq(
    # (2, "B", false, Date.valueOf("2019-01-01"), Date.valueOf("2020-01-01")),
    # (3, "C", true, Date.valueOf("2020-09-15"), null),
    # (2, "Z", true, Date.valueOf("2020-01-01"), null),
    # (4, "D", true, Date.valueOf("2019-01-01"), null),
    # (1, "A", true, Date.valueOf("2019-01-01"), null),
    # ).toDF("pkey", "attr", "cur", "effective_date", "end_date")
    # assertSmallDataFrameEquality(res, expected, orderedComparison = false, ignoreNullable = true)

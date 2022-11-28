import pytest
import chispa
import pyspark
from delta import *
from datetime import datetime as dt
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    DateType,
    TimestampType,
)
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

def describe_upsert():
    def it_upserts_with_single_attribute():
        path = "tmp/delta-upsert-single-attr"
        data2 = [
            (1, "A", True, dt(2019, 1, 1), None),
            (2, "B", True, dt(2019, 1, 1), None),
            (4, "D", True, dt(2019, 1, 1), None),
        ]
        schema = StructType(
            [
                StructField("pkey", IntegerType(), True),
                StructField("attr", StringType(), True),
                StructField("is_current", BooleanType(), True),
                StructField("effective_time", TimestampType(), True),
                StructField("end_time", TimestampType(), True),
            ]
        )
        df = spark.createDataFrame(data=data2, schema=schema)
        df.write.format("delta").save(path)

        updates_data = [
            (2, "Z", dt(2020, 1, 1)),  # value to upsert
            (3, "C", dt(2020, 9, 15)),  # new value
        ]
        updates_schema = StructType(
            [
                StructField("pkey", IntegerType(), True),
                StructField("attr", StringType(), True),
                StructField("effective_time", TimestampType(), True),
            ]
        )
        updatesDF = spark.createDataFrame(data=updates_data, schema=updates_schema)

        mack.type_2_scd_upsert(path, updatesDF, "pkey", ["attr"])

        actual_df = spark.read.format("delta").load(path)

        expected_df = spark.createDataFrame(
            [
                (2, "B", False, dt(2019, 1, 1), dt(2020, 1, 1)),
                (3, "C", True, dt(2020, 9, 15), None),
                (2, "Z", True, dt(2020, 1, 1), None),
                (4, "D", True, dt(2019, 1, 1), None),
                (1, "A", True, dt(2019, 1, 1), None),
            ],
            schema,
        )

        chispa.assert_df_equality(actual_df, expected_df, ignore_row_order=True)

    def it_errors_out_if_base_df_does_not_have_all_required_columns():
        path = "tmp/delta-incomplete"
        data2 = [
            ("A", True, dt(2019, 1, 1), None),
            ("B", True, dt(2019, 1, 1), None),
            ("D", True, dt(2019, 1, 1), None),
        ]
        schema = StructType(
            [
                StructField("attr", StringType(), True),
                StructField("is_current", BooleanType(), True),
                StructField("effective_time", TimestampType(), True),
                StructField("end_time", TimestampType(), True),
            ]
        )
        df = spark.createDataFrame(data=data2, schema=schema)
        df.write.format("delta").save(path)

        updates_data = [
            (2, "Z", dt(2020, 1, 1)),  # value to upsert
            (3, "C", dt(2020, 9, 15)),  # new value
        ]
        updates_schema = StructType(
            [
                StructField("pkey", IntegerType(), True),
                StructField("attr", StringType(), True),
                StructField("effective_time", TimestampType(), True),
            ]
        )
        updatesDF = spark.createDataFrame(data=updates_data, schema=updates_schema)

        with pytest.raises(mack.MackValidationError) as e_info:
            mack.type_2_scd_upsert(path, updatesDF, "pkey", ["attr"])


def describe_type_2_scd_generic_upsert():
    def it_upserts_based_on_date_columns():
        path = "tmp/delta-upsert-date"
        # // create Delta Lake
        data2 = [
            (1, "A", True, dt(2019, 1, 1), None),
            (2, "B", True, dt(2019, 1, 1), None),
            (4, "D", True, dt(2019, 1, 1), None),
        ]

        schema = StructType(
            [
                StructField("pkey", IntegerType(), True),
                StructField("attr", StringType(), True),
                StructField("cur", BooleanType(), True),
                StructField("effective_date", DateType(), True),
                StructField("end_date", DateType(), True),
            ]
        )

        df = spark.createDataFrame(data=data2, schema=schema)
        df.write.format("delta").save(path)

        # create updates DF
        updatesDF = spark.createDataFrame(
            [
                (3, "C", dt(2020, 9, 15)),  # new value
                (2, "Z", dt(2020, 1, 1)),  # value to upsert
            ]
        ).toDF("pkey", "attr", "effective_date")

        # perform upsert
        mack.type_2_scd_generic_upsert(path, updatesDF, "pkey", ["attr"], "cur", "effective_date", "end_date")

        actual_df = spark.read.format("delta").load(path)

        expected_df = spark.createDataFrame(
            [
                (2, "B", False, dt(2019, 1, 1), dt(2020, 1, 1)),
                (3, "C", True, dt(2020, 9, 15), None),
                (2, "Z", True, dt(2020, 1, 1), None),
                (4, "D", True, dt(2019, 1, 1), None),
                (1, "A", True, dt(2019, 1, 1), None),
            ],
            schema,
        )

        chispa.assert_df_equality(actual_df, expected_df, ignore_row_order=True)

    def it_upserts_based_on_version_number():
        path = "tmp/delta-upsert-version"
        # create Delta Lake
        data2 = [
            (1, "A", True, 1, None),
            (2, "B", True, 1, None),
            (4, "D", True, 1, None),
        ]

        schema = StructType(
            [
                StructField("pkey", IntegerType(), True),
                StructField("attr", StringType(), True),
                StructField("is_current", BooleanType(), True),
                StructField("effective_ver", IntegerType(), True),
                StructField("end_ver", IntegerType(), True),
            ]
        )

        df = spark.createDataFrame(data=data2, schema=schema)

        df.write.format("delta").save(path)

        # create updates DF
        updatesDF = spark.createDataFrame([
            (2, "Z", 2), # value to upsert
            (3, "C", 3), # new value
        ]).toDF("pkey", "attr", "effective_ver")
        
        # perform upsert
        mack.type_2_scd_generic_upsert(path, updatesDF, "pkey", ["attr"], "is_current", "effective_ver", "end_ver")
        
        # show result
        res = spark.read.format("delta").load(path)

        expected_data = [
            (2, "B", False, 1, 2),
            (3, "C", True, 3, None),
            (2, "Z", True, 2, None),
            (4, "D", True, 1, None),
            (1, "A", True, 1, None),
        ]

        expected = spark.createDataFrame(expected_data, schema)

        chispa.assert_df_equality(res, expected, ignore_row_order=True)

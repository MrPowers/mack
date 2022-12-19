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


# upsert
def test_upserts_with_single_attribute(tmp_path):
    path = f"{tmp_path}/tmp/delta-upsert-single-attr"
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


def test_errors_out_if_base_df_does_not_have_all_required_columns(tmp_path):
    path = f"{tmp_path}/tmp/delta-incomplete"
    data2 = [
        ("A", True, dt(2019, 1, 1), None),
        ("B", True, dt(2019, 1, 1), None),
        ("D", True, dt(2019, 1, 1), None),
    ]
    schema = StructType(
        [
            # pkey is missing from base!
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


def test_errors_out_if_updates_table_does_not_contain_all_required_columns(tmp_path):
    path = f"{tmp_path}/tmp/delta-error-udpate-missing-col"
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
        ("Z", dt(2020, 1, 1)),  # value to upsert
        ("C", dt(2020, 9, 15)),  # new value
    ]
    updates_schema = StructType(
        [
            # pkey is missing from updates DataFrame
            StructField("attr", StringType(), True),
            StructField("effective_time", TimestampType(), True),
        ]
    )
    updatesDF = spark.createDataFrame(data=updates_data, schema=updates_schema)

    with pytest.raises(mack.MackValidationError) as e_info:
        mack.type_2_scd_upsert(path, updatesDF, "pkey", ["attr"])


def test_upserts_based_on_multiple_attributes(tmp_path):
    path = f"{tmp_path}/tmp/delta-upsert-multiple-attr"
    data2 = [
        (1, "A", "A", True, dt(2019, 1, 1), None),
        (2, "B", "B", True, dt(2019, 1, 1), None),
        (4, "D", "D", True, dt(2019, 1, 1), None),
    ]
    schema = StructType(
        [
            StructField("pkey", IntegerType(), True),
            StructField("attr1", StringType(), True),
            StructField("attr2", StringType(), True),
            StructField("is_current", BooleanType(), True),
            StructField("effective_time", TimestampType(), True),
            StructField("end_time", TimestampType(), True),
        ]
    )
    df = spark.createDataFrame(data=data2, schema=schema)
    df.write.format("delta").save(path)

    updates_data = [
        (2, "Z", None, dt(2020, 1, 1)),  # value to upsert
        (3, "C", "C", dt(2020, 9, 15)),  # new value
    ]
    updates_schema = StructType(
        [
            StructField("pkey", IntegerType(), True),
            StructField("attr1", StringType(), True),
            StructField("attr2", StringType(), True),
            StructField("effective_time", TimestampType(), True),
        ]
    )
    updatesDF = spark.createDataFrame(data=updates_data, schema=updates_schema)

    mack.type_2_scd_upsert(path, updatesDF, "pkey", ["attr1", "attr2"])

    actual_df = spark.read.format("delta").load(path)

    expected_df = spark.createDataFrame(
        [
            (2, "B", "B", False, dt(2019, 1, 1), dt(2020, 1, 1)),
            (3, "C", "C", True, dt(2020, 9, 15), None),
            (2, "Z", None, True, dt(2020, 1, 1), None),
            (4, "D", "D", True, dt(2019, 1, 1), None),
            (1, "A", "A", True, dt(2019, 1, 1), None),
        ],
        schema,
    )

    chispa.assert_df_equality(actual_df, expected_df, ignore_row_order=True)


# def describe_type_2_scd_generic_upsert():
# type_2_scd_generic_upsert
def test_upserts_based_on_date_columns(tmp_path):
    path = f"{tmp_path}/tmp/delta-upsert-date"
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


def test_upserts_based_on_version_number(tmp_path):
    path = f"{tmp_path}/tmp/delta-upsert-version"
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
        (2, "Z", 2),  # value to upsert
        (3, "C", 3),  # new value
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


# def describe_kill_duplicates():
def test_kills_duplicates_in_a_delta_table(tmp_path):
    path = f"{tmp_path}/deduplicate1"
    data = [
        (1, "A", "A"),  # duplicate
        (2, "A", "B"),
        (3, "A", "A"),  # duplicate
        (4, "A", "A"),  # duplicate
        (5, "B", "B"),  # duplicate
        (6, "D", "D"),
        (9, "B", "B"),  # duplicate
    ]
    df = spark.createDataFrame(data, ["col1", "col2", "col3"])
    df.write.format("delta").save(path)

    deltaTable = DeltaTable.forPath(spark, path)

    mack.kill_duplicates(deltaTable, ["col3", "col2"])

    res = spark.read.format("delta").load(path)

    expected_data = [
        (2, "A", "B"),
        (6, "D", "D"),
    ]
    expected = spark.createDataFrame(expected_data, ["col1", "col2", "col3"])

    chispa.assert_df_equality(res, expected, ignore_row_order=True)


def test_drop_duplicates_in_a_delta_table(tmp_path):
    path = f"{tmp_path}/drop_duplicates"
    data = [
        (1, "A", "A", "C"),  # duplicate
        (2, "A", "B", "C"),
        (3, "A", "A", "D"),  # duplicate
        (4, "A", "A", "E"),  # duplicate
        (5, "B", "B", "C"),  # duplicate
        (6, "D", "D", "C"),
        (9, "B", "B", "E"),  # duplicate
    ]
    df = spark.createDataFrame(data, ["col1", "col2", "col3", "col4"])
    df.write.format("delta").save(path)

    deltaTable = DeltaTable.forPath(spark, path)

    mack.drop_duplicates(deltaTable, "col1", ["col2", "col3"])

    res = spark.read.format("delta").load(path)

    expected_data = [
        (1, "A", "A", "C"),
        (2, "A", "B", "C"),
        (5, "B", "B", "C"),
        (6, "D", "D", "C"),
    ]
    expected = spark.createDataFrame(expected_data, ["col1", "col2", "col3", "col4"])

    chispa.assert_df_equality(res, expected, ignore_row_order=True)


def test_copy_delta_table(tmp_path):
    path = f"{tmp_path}/copy_test_1"
    data = [
        (1, "A", "A"),
        (2, "A", "B"),
    ]
    df = spark.createDataFrame(data, ["col1", "col2", "col3"])

    (
        df
        .write
        .format("delta")
        .partitionBy(['col1'])
        .option('delta.logRetentionDuration', 'interval 30 days')
        .save(path)
    )

    origin_table = DeltaTable.forPath(spark, path)
    origin_details = origin_table.detail().select("partitionColumns", "properties")

    mack.copy_table(origin_table, f"{tmp_path}/copy_test_2")

    copied_table = DeltaTable.forPath(spark, f"{tmp_path}/copy_test_2")
    copied_details = copied_table.detail().select("partitionColumns", "properties")

    chispa.assert_df_equality(origin_details, copied_details)
    chispa.assert_df_equality(origin_table.toDF(), copied_table.toDF(), ignore_row_order=True)

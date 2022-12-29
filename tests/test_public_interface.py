import pytest
import chispa
import pyspark
from delta import DeltaTable, configure_spark_with_delta_pip
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
    updates_df = spark.createDataFrame(data=updates_data, schema=updates_schema)

    mack.type_2_scd_upsert(path, updates_df, "pkey", ["attr"])

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
    updates_df = spark.createDataFrame(data=updates_data, schema=updates_schema)

    with pytest.raises(TypeError):
        mack.type_2_scd_upsert(path, updates_df, "pkey", ["attr"])


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
    updates_df = spark.createDataFrame(data=updates_data, schema=updates_schema)

    with pytest.raises(TypeError):
        mack.type_2_scd_upsert(path, updates_df, "pkey", ["attr"])


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
    updates_df = spark.createDataFrame(data=updates_data, schema=updates_schema)

    mack.type_2_scd_upsert(path, updates_df, "pkey", ["attr1", "attr2"])

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
    updates_df = spark.createDataFrame(
        [
            (3, "C", dt(2020, 9, 15)),  # new value
            (2, "Z", dt(2020, 1, 1)),  # value to upsert
        ]
    ).toDF("pkey", "attr", "effective_date")

    # perform upsert
    mack.type_2_scd_generic_upsert(
        path, updates_df, "pkey", ["attr"], "cur", "effective_date", "end_date"
    )

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
    updates_df = spark.createDataFrame(
        [
            (2, "Z", 2),  # value to upsert
            (3, "C", 3),  # new value
        ]
    ).toDF("pkey", "attr", "effective_ver")

    # perform upsert
    mack.type_2_scd_generic_upsert(
        path, updates_df, "pkey", ["attr"], "is_current", "effective_ver", "end_ver"
    )

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

    delta_table = DeltaTable.forPath(spark, path)

    mack.kill_duplicates(delta_table, ["col3", "col2"])

    res = spark.read.format("delta").load(path)

    expected_data = [
        (2, "A", "B"),
        (6, "D", "D"),
    ]
    expected = spark.createDataFrame(expected_data, ["col1", "col2", "col3"])

    chispa.assert_df_equality(res, expected, ignore_row_order=True)


def test_drop_duplicates_pkey_in_a_delta_table(tmp_path):
    path = f"{tmp_path}/drop_duplicates_pkey"
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

    delta_table = DeltaTable.forPath(spark, path)

    mack.drop_duplicates_pkey(delta_table, "col1", ["col2", "col3"])

    res = spark.read.format("delta").load(path)

    expected_data = [
        (1, "A", "A", "C"),
        (2, "A", "B", "C"),
        (5, "B", "B", "C"),
        (6, "D", "D", "C"),
    ]
    expected = spark.createDataFrame(expected_data, ["col1", "col2", "col3", "col4"])

    chispa.assert_df_equality(res, expected, ignore_row_order=True)


def test_drop_duplicates_pkey_in_a_delta_table_no_duplication_cols(tmp_path):
    path = f"{tmp_path}/drop_duplicates_pkey_no_duplication_cols"
    data = [
        (1, "A", "A", "C"),  # duplicate
        (1, "A", "A", "C"),  # duplicate
        (1, "A", "A", "C"),  # duplicate
        (1, "A", "A", "C"),  # duplicate
    ]
    df = spark.createDataFrame(data, ["col1", "col2", "col3", "col4"])
    df.write.format("delta").save(path)

    delta_table = DeltaTable.forPath(spark, path)

    with pytest.raises(TypeError):
        mack.drop_duplicates_pkey(delta_table, "col1", [])


def test_drop_duplicates_in_a_delta_table(tmp_path):
    path = f"{tmp_path}/drop_duplicates"
    data = [
        (1, "A", "A", "C"),  # duplicate
        (1, "A", "A", "C"),  # duplicate
        (1, "A", "A", "C"),  # duplicate
        (1, "A", "A", "C"),  # duplicate
    ]
    df = spark.createDataFrame(data, ["col1", "col2", "col3", "col4"])
    df.write.format("delta").save(path)

    delta_table = DeltaTable.forPath(spark, path)

    mack.drop_duplicates(delta_table, ["col1"]),

    res = spark.read.format("delta").load(path)

    expected_data = [
        (1, "A", "A", "C"),
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
        df.write.format("delta")
        .partitionBy(["col1"])
        .option("delta.logRetentionDuration", "interval 30 days")
        .save(path)
    )

    origin_table = DeltaTable.forPath(spark, path)
    origin_details = origin_table.detail().select("partitionColumns", "properties")

    mack.copy_table(origin_table, f"{tmp_path}/copy_test_2")

    copied_table = DeltaTable.forPath(spark, f"{tmp_path}/copy_test_2")
    copied_details = copied_table.detail().select("partitionColumns", "properties")

    chispa.assert_df_equality(origin_details, copied_details)
    chispa.assert_df_equality(
        origin_table.toDF(), copied_table.toDF(), ignore_row_order=True
    )


# append without duplicates
def test_append_without_duplicates_single_column(tmp_path):
    path = f"{tmp_path}/append_without_duplicates"
    data = [
        (1, "A", "B"),
        (2, "C", "D"),
        (3, "E", "F"),
    ]
    df = spark.createDataFrame(data, ["col1", "col2", "col3"])
    df.write.format("delta").save(path)

    delta_table = DeltaTable.forPath(spark, path)

    append_df = spark.createDataFrame(
        [
            (2, "R", "T"),  # duplicate
            (8, "A", "B"),
            (10, "X", "Y"),
        ],
        ["col1", "col2", "col3"],
    )

    mack.append_without_duplicates(delta_table, append_df, ["col1"])

    appended_data = spark.read.format("delta").load(path)

    expected_data = [
        (1, "A", "B"),
        (2, "C", "D"),
        (3, "E", "F"),
        (8, "A", "B"),
        (10, "X", "Y"),
    ]
    expected = spark.createDataFrame(expected_data, ["col1", "col2", "col3"])
    chispa.assert_df_equality(appended_data, expected, ignore_row_order=True)


def test_append_without_duplicates_multi_column(tmp_path):
    path = f"{tmp_path}/append_without_duplicates"
    data = [
        (1, "a", "A"),
        (2, "b", "R"),
        (3, "c", "X"),
    ]
    df = spark.createDataFrame(data, ["col1", "col2", "col3"])
    df.write.format("delta").save(path)

    delta_table = DeltaTable.forPath(spark, path)

    append_data = spark.createDataFrame(
        [
            (2, "b", "R"),  # duplicate col1, col2
            (2, "x", "R"),  # NOT duplicate col1, col2
            (8, "y", "F"),
            (10, "z", "U"),
        ],
        ["col1", "col2", "col3"],
    )

    mack.append_without_duplicates(delta_table, append_data, ["col1", "col2"])

    appended_data = spark.read.format("delta").load(path)

    expected_data = [
        (1, "a", "A"),
        (2, "b", "R"),
        (2, "x", "R"),
        (3, "c", "X"),
        (8, "y", "F"),
        (10, "z", "U"),
    ]
    expected = spark.createDataFrame(expected_data, ["col1", "col2", "col3"])
    chispa.assert_df_equality(appended_data, expected, ignore_row_order=True)


def test_is_composite_key_candidate(tmp_path):
    path = f"{tmp_path}/is_composite_key_candidate"
    data = [
        (1, "a", "A"),
        (2, "b", "R"),
        (2, "c", "D"),
        (3, "e", "F"),
    ]

    df = spark.createDataFrame(data, ["col1", "col2", "col3"])
    df.write.format("delta").save(path)

    delta_table = DeltaTable.forPath(spark, path)

    assert not mack.is_composite_key_candidate(delta_table, ["col1"])
    assert mack.is_composite_key_candidate(delta_table, ["col1", "col2"])


def test_describe_table(tmp_path):
    path = f"{tmp_path}/copy_test_1"
    data = [
        (1, "A", "A"),
        (2, "A", "B"),
    ]
    df = spark.createDataFrame(data, ["col1", "col2", "col3"])

    (
        df.write.format("delta")
        .partitionBy(["col1"])
        .option("delta.logRetentionDuration", "interval 30 days")
        .save(path)
    )

    delta_table = DeltaTable.forPath(spark, path)

    result = mack.delta_file_sizes(delta_table)

    expected_result = {
        "size_in_bytes": 1320,
        "number_of_files": 2,
        "average_file_size_in_bytes": 660,
    }

    assert result == expected_result


def test_humanize_bytes_formats_nicely():
    assert mack.humanize_bytes(12345678) == "12.35 MB"
    assert mack.humanize_bytes(1234567890) == "1.23 GB"
    assert mack.humanize_bytes(1234567890000) == "1.23 TB"
    assert mack.humanize_bytes(1234567890000000) == "1.23 PB"


def test_find_composite_key(tmp_path):
    path = f"{tmp_path}/find_composite_key"
    data = [
        (
            'c054f1c7-3765-49d6-aa76-debd6e76691c', 'users_bronze_dlt', 'correct_schema', 60000, 0, 'COMPLETED', 0,
            1000000,
            '2021-10-06T14:07:00.000+0000'),
        ('d5d76478-ff24-4bca-aede-c69f31b5b35e', 'user_silver_dlt', 'valid_id', 50000, 400, 'COMPLETED', 0, 1000000,
         '2021-10-06T14:07:00.000+0000'),
        (
            '4b07c459-f414-492a-9f80-640a741c12c6', 'user_gold_dlt', 'valid_income', 60000, 1600, 'COMPLETED', 1600,
            100000,
            '2021-10-07T14:02:00.000+0000'),
        ('c054f1c7-3765-49d6-aa76-debd6e76691c', 'spend_silver_dlt', 'valid_id', 70000, 500, 'COMPLETED', 0, 1000000,
         '2021-10-08T14:09:00.000+0000'),
        ('c054f1c7-3765-49d6-aa76-debd6e76691c', 'users_bronze_dlt', 'correct_schema', 70000, 1000, 'COMPLETED', 0,
         1000000, '2021-10-08T14:09:00.000+0000'),
        ('d5d76478-ff24-4bca-aede-c69f31b5b35e', 'user_silver_dlt', 'valid_id', 60000, 1400, 'COMPLETED', 0, 1000000,
         '2021-10-08T14:09:00.000+0000'),
        ('4b07c459-f414-492a-9f80-640a741c12c6', 'user_gold_dlt', 'valid_age', 60000, 1600, 'COMPLETED', 1600, 100000,
         '2021-10-08T14:09:00.000+0000'),
        ('4b07c459-f414-492a-9f80-640a741c12c6', 'user_gold_dlt', 'valid_score', 60000, 1600, 'COMPLETED', 1600, 100000,
         '2021-10-08T14:09:00.000+0000'),
    ]
    df = spark.createDataFrame(data, ['id', 'dataset', 'name', 'passed_records', 'failed_records', 'status_update',
                                      'dropped_records', 'output_records', 'timestamp'
                                      ])
    df.write.format("delta").save(path)

    delta_table = DeltaTable.forPath(spark, path)

    composite_keys = mack.find_composite_key(delta_table.toDF(), ['passed_records', 'failed_records', 'status_update',
                                                                  'dropped_records', 'output_records'])

    expected_keys = ['id', 'name', 'timestamp']

    assert composite_keys == expected_keys


def test_get_md5(tmp_path):
    path = f"{tmp_path}/find_composite_key"
    data = [
        (
            'c054f1c7-3765-49d6-aa76-debd6e76691c', 'users_bronze_dlt', 'correct_schema', 60000, 0, 'COMPLETED', 0,
            1000000,
            '2021-10-06T14:07:00.000+0000'),
        ('d5d76478-ff24-4bca-aede-c69f31b5b35e', 'user_silver_dlt', 'valid_id', 50000, 400, 'COMPLETED', 0, 1000000,
         '2021-10-06T14:07:00.000+0000'),
        (
            '4b07c459-f414-492a-9f80-640a741c12c6', 'user_gold_dlt', 'valid_income', 60000, 1600, 'COMPLETED', 1600,
            100000,
            '2021-10-07T14:02:00.000+0000'),
        ('c054f1c7-3765-49d6-aa76-debd6e76691c', 'spend_silver_dlt', 'valid_id', 70000, 500, 'COMPLETED', 0, 1000000,
         '2021-10-08T14:09:00.000+0000'),
        ('c054f1c7-3765-49d6-aa76-debd6e76691c', 'users_bronze_dlt', 'correct_schema', 70000, 1000, 'COMPLETED', 0,
         1000000, '2021-10-08T14:09:00.000+0000'),
        ('d5d76478-ff24-4bca-aede-c69f31b5b35e', 'user_silver_dlt', 'valid_id', 60000, 1400, 'COMPLETED', 0, 1000000,
         '2021-10-08T14:09:00.000+0000'),
        ('4b07c459-f414-492a-9f80-640a741c12c6', 'user_gold_dlt', 'valid_age', 60000, 1600, 'COMPLETED', 1600, 100000,
         '2021-10-08T14:09:00.000+0000'),
        ('4b07c459-f414-492a-9f80-640a741c12c6', 'user_gold_dlt', 'valid_score', 60000, 1600, 'COMPLETED', 1600, 100000,
         '2021-10-08T14:09:00.000+0000'),
    ]
    df = spark.createDataFrame(data, ['id', 'dataset', 'name', 'passed_records', 'failed_records', 'status_update',
                                      'dropped_records', 'output_records', 'timestamp'
                                      ])
    df.write.format("delta").save(path)

    delta_table = DeltaTable.forPath(spark, path)
    with_md5 = mack.get_md5(delta_table.toDF(),['id', 'name', 'timestamp'])

    expected_data = [
        (
        'c054f1c7-3765-49d6-aa76-debd6e76691c', 'users_bronze_dlt', 'correct_schema', 60000, 0, 'COMPLETED', 0, 1000000,
        '2021-10-06T14:07:00.000+0000', 'e0d7b4c7c7f36e5b14a14455707868e7'),
        ('d5d76478-ff24-4bca-aede-c69f31b5b35e', 'user_silver_dlt', 'valid_id', 50000, 400, 'COMPLETED', 0, 1000000,
         '2021-10-06T14:07:00.000+0000', '3d3ec278e10ac253a563612f2536ebb2'),
        (
        '4b07c459-f414-492a-9f80-640a741c12c6', 'user_gold_dlt', 'valid_income', 60000, 1600, 'COMPLETED', 1600, 100000,
        '2021-10-07T14:02:00.000+0000', 'b21347fb9cb04fa092f560487dffef4f'),
        ('c054f1c7-3765-49d6-aa76-debd6e76691c', 'spend_silver_dlt', 'valid_id', 70000, 500, 'COMPLETED', 0, 1000000,
         '2021-10-08T14:09:00.000+0000', 'f1177476e14a5a4032c4870304ef7183'),
        ('c054f1c7-3765-49d6-aa76-debd6e76691c', 'users_bronze_dlt', 'correct_schema', 70000, 1000, 'COMPLETED', 0,
         1000000, '2021-10-08T14:09:00.000+0000', '8cece0c20cfa3a596361f8331336c567'),
        ('d5d76478-ff24-4bca-aede-c69f31b5b35e', 'user_silver_dlt', 'valid_id', 60000, 1400, 'COMPLETED', 0, 1000000,
         '2021-10-08T14:09:00.000+0000', 'e465948ee1bba6e7e90b4c32eddf2867'),
        ('4b07c459-f414-492a-9f80-640a741c12c6', 'user_gold_dlt', 'valid_age', 60000, 1600, 'COMPLETED', 1600, 100000,
         '2021-10-08T14:09:00.000+0000', 'acbfca92d159c94b4cf2750c2642d591'),
        ('4b07c459-f414-492a-9f80-640a741c12c6', 'user_gold_dlt', 'valid_score', 60000, 1600, 'COMPLETED', 1600, 100000,
         '2021-10-08T14:09:00.000+0000', '79d22ad6a8e43dbc45b4681d78ef7f6b'),
    ]
    expected = spark.createDataFrame(expected_data,
                                     ['id', 'dataset', 'name', 'passed_records', 'failed_records', 'status_update',
                                      'dropped_records', 'output_records', 'timestamp', 'md5'])
    chispa.assert_df_equality(with_md5, expected, ignore_row_order=True, ignore_schema=True)

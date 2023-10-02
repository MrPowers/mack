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
    .config("spark.sql.shuffle.partitions", "2")
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

    delta_table = DeltaTable.forPath(spark, path)
    mack.type_2_scd_upsert(delta_table, updates_df, "pkey", ["attr"])

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

    delta_table = DeltaTable.forPath(spark, path)
    with pytest.raises(TypeError):
        mack.type_2_scd_upsert(delta_table, updates_df, "pkey", ["attr"])


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

    delta_table = DeltaTable.forPath(spark, path)
    with pytest.raises(TypeError):
        mack.type_2_scd_upsert(delta_table, updates_df, "pkey", ["attr"])


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

    delta_table = DeltaTable.forPath(spark, path)
    mack.type_2_scd_upsert(delta_table, updates_df, "pkey", ["attr1", "attr2"])

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
    delta_table = DeltaTable.forPath(spark, path)
    mack.type_2_scd_generic_upsert(
        delta_table, updates_df, "pkey", ["attr"], "cur", "effective_date", "end_date"
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
    delta_table = DeltaTable.forPath(spark, path)
    mack.type_2_scd_generic_upsert(
        delta_table,
        updates_df,
        "pkey",
        ["attr"],
        "is_current",
        "effective_ver",
        "end_ver",
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


def test_upserts_does_not_insert_duplicate(tmp_path):
    path = f"{tmp_path}/tmp/delta-no-duplicate"
    # create Delta Lake
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
            (1, "A", dt(2019, 1, 1)),  # duplicate row
        ]
    ).toDF("pkey", "attr", "effective_date")

    # perform upsert
    delta_table = DeltaTable.forPath(spark, path)
    mack.type_2_scd_generic_upsert(
        delta_table, updates_df, "pkey", ["attr"], "cur", "effective_date", "end_date"
    )

    actual_df = spark.read.format("delta").load(path)

    expected_df = spark.createDataFrame(
        [
            (1, "A", True, dt(2019, 1, 1), None),
            (2, "B", True, dt(2019, 1, 1), None),
            (4, "D", True, dt(2019, 1, 1), None),
        ],
        schema,
    )

    chispa.assert_df_equality(actual_df, expected_df, ignore_row_order=True)


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
            (8, "B", "C"),  # duplicate
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


def test_validate_append(tmp_path):
    path = f"{tmp_path}/validate_append"

    def append_fun(delta_table, append_df):
        mack.validate_append(
            delta_table,
            append_df,
            required_cols=["col1", "col2"],
            optional_cols=["col4"],
        )

    # Create Delta table
    data = [
        (1, "a", "A"),
        (2, "b", "B"),
    ]
    df = spark.createDataFrame(data, ["col1", "col2", "col3"])
    df.write.format("delta").save(path)

    # Demonstrate that certain DataFrames with optional columns can be appended
    delta_table = DeltaTable.forPath(spark, path)
    append_df = spark.createDataFrame(
        [
            (3, "c", "cat"),
            (4, "d", "dog"),
        ],
        ["col1", "col2", "col4"],
    )
    append_fun(delta_table, append_df)

    expected_data = [
        (1, "a", "A", None),
        (2, "b", "B", None),
        (3, "c", None, "cat"),
        (4, "d", None, "dog"),
    ]
    expected = spark.createDataFrame(expected_data, ["col1", "col2", "col3", "col4"])
    chispa.assert_df_equality(
        spark.read.format("delta").load(path), expected, ignore_row_order=True
    )

    # demonstrate that DataFrames with columns that are not on the accept list cannot be appended
    append_df = spark.createDataFrame(
        [
            (4, "b", "A"),
            (5, "y", "C"),
            (6, "z", "D"),
        ],
        ["col1", "col2", "col5"],
    )
    with pytest.raises(TypeError):
        mack.validate_append(
            delta_table,
            append_df,
            required_cols=["col1", "col2"],
            optional_cols=["col4"],
        )

    # demonstrate that DataFrames with missing required columns cannot be appended
    append_df = spark.createDataFrame(
        [
            (4, "A"),
            (5, "C"),
            (6, "D"),
        ],
        ["col1", "col4"],
    )
    with pytest.raises(TypeError):
        mack.validate_append(
            delta_table,
            append_df,
            required_cols=["col1", "col2"],
            optional_cols=["col4"],
        )


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


def test_delta_file_sizes(tmp_path):
    path = f"{tmp_path}/delta_file_sizes"
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


def test_show_delta_file_sizes(capfd, tmp_path):
    path = f"{tmp_path}/show_delta_file_sizes"
    data = [
        (1, "A", "A"),
        (2, "A", "B"),
    ]
    df = spark.createDataFrame(data, ["col1", "col2", "col3"])

    (df.write.format("delta").partitionBy(["col1"]).save(path))

    delta_table = DeltaTable.forPath(spark, path)

    mack.show_delta_file_sizes(delta_table)

    out, _ = capfd.readouterr()

    assert (
        out
        == "The delta table contains 2 files with a size of 1.32 kB. The average file size is 660.0 B\n"
    )


def test_humanize_bytes_formats_nicely():
    assert mack.humanize_bytes(12345678) == "12.35 MB"
    assert mack.humanize_bytes(1234567890) == "1.23 GB"
    assert mack.humanize_bytes(1234567890000) == "1.23 TB"
    assert mack.humanize_bytes(1234567890000000) == "1.23 PB"


def test_humanize_bytes_binary_formats_nicely():
    assert mack.humanize_bytes_binary(12345678) == "11.77 MB"
    assert mack.humanize_bytes_binary(1234567890) == "1.15 GB"
    assert mack.humanize_bytes_binary(1234567890000) == "1.12 TB"
    assert mack.humanize_bytes_binary(1234567890000000) == "1.10 PB"


def test_find_composite_key(tmp_path):
    path = f"{tmp_path}/find_composite_key"
    data = [
        (1, "a", "z"),
        (1, "a", "b"),
        (3, "c", "b"),
    ]
    df = spark.createDataFrame(
        data,
        [
            "col1",
            "col2",
            "col3",
        ],
    )
    df.write.format("delta").save(path)

    delta_table = DeltaTable.forPath(spark, path)

    composite_keys = mack.find_composite_key_candidates(delta_table)

    expected_keys = ["col1", "col3"]

    assert composite_keys == expected_keys


def test_find_composite_key_with_value_error(tmp_path):
    path = f"{tmp_path}/find_composite_key"
    data = [
        (1, "a", "A"),
        (2, "b", "R"),
        (2, "c", "D"),
        (3, "e", "F"),
    ]
    df = spark.createDataFrame(data, ["col1", "col2", "col3"])
    df.write.format("delta").save(path)

    delta_table = DeltaTable.forPath(spark, path)
    with pytest.raises(
        ValueError, match="No composite key candidates could be identified."
    ):
        mack.find_composite_key_candidates(delta_table, ["col2", "col3"])


def test_with_md5_cols(tmp_path):
    path = f"{tmp_path}/find_composite_key"
    data = [
        (1, "a", None),
        (2, "b", "b"),
        (3, "c", "c"),
    ]
    df = spark.createDataFrame(
        data,
        [
            "col1",
            "col2",
            "col3",
        ],
    )
    df.write.format("delta").save(path)

    delta_table = DeltaTable.forPath(spark, path)
    with_md5 = mack.with_md5_cols(delta_table, ["col2", "col3"])

    expected_data = [
        (1, "a", None, "0cc175b9c0f1b6a831c399e269772661"),
        (2, "b", "b", "1eeaac3814eb80cc40efb005cf0b9141"),
        (3, "c", "c", "4e202f8309e7b00349c70845ab02fce9"),
    ]
    expected_df = spark.createDataFrame(
        expected_data,
        ["col1", "col2", "col3", "md5_col2_col3"],
    )
    chispa.assert_df_equality(
        with_md5, expected_df, ignore_row_order=True, ignore_schema=True
    )


def test_lastest_version(tmp_path):
    path = f"{tmp_path}/latestversion"

    data = [
        (1, "a", None),
        (2, "b", "b"),
        (3, "c", "c"),
    ]
    df = spark.createDataFrame(
        data,
        [
            "col1",
            "col2",
            "col3",
        ],
    )
    df.write.format("delta").save(path)

    # write the same dataframe twice
    df.write.format("delta").mode("append").save(path)
    df.write.format("delta").mode("append").save(path)

    delta_table = DeltaTable.forPath(spark, path)
    latest_version = mack.latest_version(delta_table)
    assert latest_version == 2


def test_constraint_append_no_constraint(tmp_path):

    target_path = f"{tmp_path}/constraint_append/target_table"
    quarantine_path = f"{tmp_path}/constraint_append/quarantine_table"

    data = [
        (1, "A", "B"),
        (2, "C", "D"),
        (3, "E", "F"),
    ]

    df = spark.createDataFrame(data, ["col1", "col2", "col3"])
    df.write.format("delta").save(target_path)

    df2 = spark.createDataFrame([], df.schema)
    df2.write.format("delta").save(quarantine_path)

    target_table = DeltaTable.forPath(spark, target_path)
    append_df = spark.createDataFrame([], df.schema)
    quarantine_table = DeltaTable.forPath(spark, quarantine_path)

    # demonstrate that the function cannot be run with target table not having constraints
    with pytest.raises(
        TypeError, match="There are no constraints present in the target delta table"
    ):
        mack.constraint_append(target_table, append_df, quarantine_table)


def test_constraint_append_multi_constraint(tmp_path):

    target_path = f"{tmp_path}/constraint_append/target_table"
    quarantine_path = f"{tmp_path}/constraint_append/quarantine_table"

    data = [
        (1, "A", "B"),
        (2, "C", "D"),
        (3, "E", "F"),
    ]
    df = spark.createDataFrame(data, ["col1", "col2", "col3"])

    df.write.format("delta").save(target_path)

    df2 = spark.createDataFrame([], df.schema)
    df2.write.format("delta").save(quarantine_path)

    target_table = DeltaTable.forPath(spark, target_path)

    # adding two constraints
    spark.sql(
        f"ALTER TABLE delta.`{target_path}` ADD CONSTRAINT col1_constraint CHECK (col1 > 0) "
    )
    spark.sql(
        f"ALTER TABLE delta.`{target_path}` ADD CONSTRAINT col2_constraint CHECK (col2 != 'Z') "
    )

    # adding other table properties
    spark.sql(
        f"ALTER TABLE delta.`{target_path}` SET TBLPROPERTIES('this.is.my.key' = 12, this.is.my.key2 = true)"
    )

    append_data = [
        (0, "Z", "Z"),
        (4, "A", "B"),
        (5, "C", "D"),
        (6, "E", "F"),
        (9, "G", "G"),
        (11, "Z", "Z"),
    ]
    append_df = spark.createDataFrame(append_data, ["col1", "col2", "col3"])

    # testing with two constraints
    target_table = DeltaTable.forPath(spark, target_path)
    quarantine_table = DeltaTable.forPath(spark, quarantine_path)
    mack.constraint_append(target_table, append_df, quarantine_table)

    expected_data = [
        (1, "A", "B"),
        (2, "C", "D"),
        (3, "E", "F"),
        (4, "A", "B"),
        (5, "C", "D"),
        (6, "E", "F"),
        (9, "G", "G"),
    ]
    expected_df = spark.createDataFrame(expected_data, ["col1", "col2", "col3"])

    appended_data = spark.read.format("delta").load(target_path)
    chispa.assert_df_equality(appended_data, expected_df, ignore_row_order=True)

    expected_quarantined_data = [(0, "Z", "Z"), (11, "Z", "Z")]
    expected_quarantined_df = spark.createDataFrame(
        expected_quarantined_data, ["col1", "col2", "col3"]
    )

    quarantined_data = spark.read.format("delta").load(quarantine_path)
    chispa.assert_df_equality(
        quarantined_data, expected_quarantined_df, ignore_row_order=True
    )


def test_constraint_append_single_constraint(tmp_path):

    target_path = f"{tmp_path}/constraint_append/target_table"
    quarantine_path = f"{tmp_path}/constraint_append/quarantine_table"

    data = [
        (1, "A", "B"),
        (2, "C", "D"),
        (3, "E", "F"),
    ]
    df = spark.createDataFrame(data, ["col1", "col2", "col3"])

    df.write.format("delta").save(target_path)

    df2 = spark.createDataFrame([], df.schema)
    df2.write.format("delta").save(quarantine_path)

    target_table = DeltaTable.forPath(spark, target_path)

    # adding two constraints
    spark.sql(
        f"ALTER TABLE delta.`{target_path}` ADD CONSTRAINT col1_constraint CHECK (col1 > 0) "
    )

    append_data = [
        (0, "Z", "Z"),
        (4, "A", "B"),
        (5, "C", "D"),
        (6, "E", "F"),
        (9, "G", "G"),
        (11, "Z", "Z"),
    ]
    append_df = spark.createDataFrame(append_data, ["col1", "col2", "col3"])

    # testing with two constraints
    target_table = DeltaTable.forPath(spark, target_path)
    quarantine_table = DeltaTable.forPath(spark, quarantine_path)
    mack.constraint_append(target_table, append_df, quarantine_table)

    expected_data = [
        (1, "A", "B"),
        (2, "C", "D"),
        (3, "E", "F"),
        (4, "A", "B"),
        (5, "C", "D"),
        (6, "E", "F"),
        (9, "G", "G"),
        (11, "Z", "Z"),
    ]
    expected_df = spark.createDataFrame(expected_data, ["col1", "col2", "col3"])

    appended_data = spark.read.format("delta").load(target_path)
    chispa.assert_df_equality(appended_data, expected_df, ignore_row_order=True)

    expected_quarantined_data = [(0, "Z", "Z")]
    expected_quarantined_df = spark.createDataFrame(
        expected_quarantined_data, ["col1", "col2", "col3"]
    )

    quarantined_data = spark.read.format("delta").load(quarantine_path)
    chispa.assert_df_equality(
        quarantined_data, expected_quarantined_df, ignore_row_order=True
    )


def test_constraint_append_notnull_constraint(tmp_path):

    target_path = f"{tmp_path}/constraint_append/target_table"
    quarantine_path = f"{tmp_path}/constraint_append/quarantine_table"

    target_schema = StructType(
        [
            StructField("col1", IntegerType(), False),
            StructField("col2", StringType(), True),
            StructField("col3", StringType(), False),
        ]
    )

    df = spark.createDataFrame([], target_schema)

    target_table = (
        DeltaTable.create(spark).location(target_path).addColumns(df.schema).execute()
    )

    quarantine_schema = StructType(
        [
            StructField("col1", IntegerType(), True),
            StructField("col2", StringType(), True),
            StructField("col3", StringType(), True),
        ]
    )

    qdf = spark.createDataFrame([], quarantine_schema)

    quarantine_table = (
        DeltaTable.create(spark)
        .location(quarantine_path)
        .addColumns(qdf.schema)
        .execute()
    )

    data = [(None, "A", "B"), (2, "C", None), (3, "E", "F"), (4, "G", "H")]
    append_df = spark.createDataFrame(data, quarantine_schema)

    mack.constraint_append(target_table, append_df, quarantine_table)

    # target data equality check
    expected_data = [(3, "E", "F"), (4, "G", "H")]
    expected_df = spark.createDataFrame(expected_data, target_schema)

    appended_data = spark.read.format("delta").load(target_path)
    chispa.assert_df_equality(appended_data, expected_df, ignore_row_order=True)

    # quarantined data equality check
    expected_quarantined_data = [
        (None, "A", "B"),
        (2, "C", None),
    ]
    expected_quarantined_df = spark.createDataFrame(
        expected_quarantined_data, quarantine_schema
    )

    quarantined_data = spark.read.format("delta").load(quarantine_path)
    chispa.assert_df_equality(
        quarantined_data, expected_quarantined_df, ignore_row_order=True
    )


def test_constraint_append_notnull_and_check_constraint(tmp_path):

    target_path = f"{tmp_path}/constraint_append/target_table"
    quarantine_path = f"{tmp_path}/constraint_append/quarantine_table"

    target_schema = StructType(
        [
            StructField("col1", IntegerType(), False),
            StructField("col2", StringType(), True),
            StructField("col3", StringType(), False),
        ]
    )

    df = spark.createDataFrame([], target_schema)

    target_table = (
        DeltaTable.create(spark)
        .location(target_path)
        .addColumns(df.schema)
        .property("delta.constraints.col1_constraint", "col1 > 0")
        .execute()
    )

    quarantine_schema = StructType(
        [
            StructField("col1", IntegerType(), True),
            StructField("col2", StringType(), True),
            StructField("col3", StringType(), True),
        ]
    )

    qdf = spark.createDataFrame([], quarantine_schema)

    quarantine_table = (
        DeltaTable.create(spark)
        .location(quarantine_path)
        .addColumns(qdf.schema)
        .execute()
    )

    data = [
        (0, "A", "B"),
        (0, "A", None),
        (None, "A", "B"),
        (2, "C", None),
        (3, "E", "F"),
        (4, "G", "H"),
    ]
    append_df = spark.createDataFrame(data, quarantine_schema)

    mack.constraint_append(target_table, append_df, quarantine_table)

    # target data equality check
    expected_data = [(3, "E", "F"), (4, "G", "H")]
    expected_df = spark.createDataFrame(expected_data, target_schema)

    appended_data = spark.read.format("delta").load(target_path)
    chispa.assert_df_equality(appended_data, expected_df, ignore_row_order=True)

    # quarantined data equality check
    expected_quarantined_data = [
        (0, "A", "B"),
        (0, "A", None),
        (None, "A", "B"),
        (2, "C", None),
    ]
    expected_quarantined_df = spark.createDataFrame(
        expected_quarantined_data, quarantine_schema
    )

    quarantined_data = spark.read.format("delta").load(quarantine_path)
    chispa.assert_df_equality(
        quarantined_data, expected_quarantined_df, ignore_row_order=True
    )


def test_rename_delta_table(tmp_path):
    data = [("Alice", 1), ("Bob", 2)]
    df = spark.createDataFrame(data, ["Name", "Age"])
    old_table_name = "old_table"
    df.write.mode("overwrite").format("delta").saveAsTable(old_table_name)

    new_table_name = "new_table"
    mack.rename_delta_table(old_table_name, new_table_name, spark)

    # Verify the table has been renamed
    assert spark._jsparkSession.catalog().tableExists(new_table_name)

    # Clean up: Drop the new table
    spark.sql(f"DROP TABLE IF EXISTS {new_table_name}")

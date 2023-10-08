from itertools import combinations
from typing import List, Union, Dict, Optional

from delta import DeltaTable
import pyspark
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, concat_ws, count, md5, row_number, max
from pyspark.sql.window import Window


def type_2_scd_upsert(
    delta_table: DeltaTable,
    updates_df: DataFrame,
    primary_key: str,
    attr_col_names: List[str],
) -> None:
    """
    <description>

    :param path: <description>
    :type path: DeltaTable
    :param updates_df: <description>
    :type updates_df: DataFrame
    :param primary_key: <description>
    :type primary_key: str
    :param attr_col_names: <description>
    :type attr_col_names: List[str]

    :returns: <description>
    :rtype: None
    """
    return type_2_scd_generic_upsert(
        delta_table,
        updates_df,
        primary_key,
        attr_col_names,
        "is_current",
        "effective_time",
        "end_time",
    )


def type_2_scd_generic_upsert(
    delta_table: DeltaTable,
    updates_df: DataFrame,
    primary_key: str,
    attr_col_names: List[str],
    is_current_col_name: str,
    effective_time_col_name: str,
    end_time_col_name: str,
) -> None:
    """
    <description>

    :param delta_table: DeltaTable
    :type path: str
    :param updates_df: <description>
    :type updates_df: DataFrame
    :param primary_key: <description>
    :type primary_key: str
    :param attr_col_names: <description>
    :type attr_col_names: List[str]
    :param is_current_col_name: <description>
    :type is_current_col_name: str
    :param effective_time_col_name: <description>
    :type effective_time_col_name: str
    :param end_time_col_name: <description>
    :type effective_time_col_name: str

    :raises TypeError: Raises type error when required column names are not in the base table.
    :raises TypeError: Raises type error when required column names for updates are not in the attributes columns list.

    :returns: <description>
    :rtype: None
    """

    # validate the existing Delta table
    base_col_names = delta_table.toDF().columns
    required_base_col_names = (
        [primary_key]
        + attr_col_names
        + [is_current_col_name, effective_time_col_name, end_time_col_name]
    )
    if sorted(base_col_names) != sorted(required_base_col_names):
        raise TypeError(
            f"The base table has these columns {base_col_names!r}, but these columns are required {required_base_col_names!r}"
        )
    # validate the updates DataFrame
    updates_col_names = updates_df.columns
    required_updates_col_names = (
        [primary_key] + attr_col_names + [effective_time_col_name]
    )
    if sorted(updates_col_names) != sorted(required_updates_col_names):
        raise TypeError(
            f"The updates DataFrame has these columns {updates_col_names!r}, but these columns are required {required_updates_col_names!r}"
        )

    # perform the upsert
    updates_attrs = list(
        map(lambda attr: f"updates.{attr} <> base.{attr}", attr_col_names)
    )
    updates_attrs = " OR ".join(updates_attrs)
    staged_updates_attrs = list(
        map(lambda attr: f"staged_updates.{attr} <> base.{attr}", attr_col_names)
    )
    staged_updates_attrs = " OR ".join(staged_updates_attrs)
    staged_part_1 = (
        updates_df.alias("updates")
        .join(delta_table.toDF().alias("base"), primary_key)
        .where(f"base.{is_current_col_name} = true AND ({updates_attrs})")
        .selectExpr("NULL as mergeKey", "updates.*")
    )
    staged_part_2 = updates_df.selectExpr(f"{primary_key} as mergeKey", "*")
    staged_updates = staged_part_1.union(staged_part_2)
    thing = {}
    for attr in attr_col_names:
        thing[attr] = f"staged_updates.{attr}"
    thing2 = {
        primary_key: f"staged_updates.{primary_key}",
        is_current_col_name: "true",
        effective_time_col_name: f"staged_updates.{effective_time_col_name}",
        end_time_col_name: "null",
    }
    res_thing = {**thing, **thing2}
    res = (
        delta_table.alias("base")
        .merge(
            source=staged_updates.alias("staged_updates"),
            condition=pyspark.sql.functions.expr(f"base.{primary_key} = mergeKey"),
        )
        .whenMatchedUpdate(
            condition=f"base.{is_current_col_name} = true AND ({staged_updates_attrs})",
            set={
                is_current_col_name: "false",
                end_time_col_name: f"staged_updates.{effective_time_col_name}",
            },
        )
        .whenNotMatchedInsert(values=res_thing)
        .execute()
    )
    return res


def kill_duplicates(delta_table: DeltaTable, duplication_columns: List[str]) -> None:
    """
    <description>

    :param delta_table: <description>
    :type delta_table: DeltaTable
    :param duplication_columns: <description>
    :type duplication_columns: List[str]

    :raises TypeError: Raises type error when input arguments have a invalid type or are empty.
    :raises TypeError: Raises type error when required columns are missing in the provided delta table.
    """
    if not isinstance(delta_table, DeltaTable):
        raise TypeError("An existing delta table must be specified.")

    if not duplication_columns or len(duplication_columns) == 0:
        raise TypeError("Duplication columns must be specified")

    data_frame = delta_table.toDF()

    # Make sure that all the required columns are present in the provided delta table
    append_data_columns = data_frame.columns
    for required_column in duplication_columns:
        if required_column not in append_data_columns:
            raise TypeError(
                f"The base table has these columns {append_data_columns!r}, but these columns are required {duplication_columns!r}"
            )

    q = []

    duplicate_records = (
        data_frame.withColumn(
            "amount_of_records",
            count("*").over(Window.partitionBy(duplication_columns)),
        )
        .filter(col("amount_of_records") > 1)
        .drop("amount_of_records")
        .distinct()
    )

    for column in duplication_columns:
        q.append(f"old.{column} = new.{column}")

    q = " AND ".join(q)

    # Remove all the duplicate records
    delta_table.alias("old").merge(
        duplicate_records.alias("new"), q
    ).whenMatchedDelete().execute()


def drop_duplicates_pkey(
    delta_table: DeltaTable, primary_key: str, duplication_columns: List[str]
) -> None:
    """
    <description>

    :param delta_table: <description>
    :type delta_table: DeltaTable
    :param primary_key: <description>
    :type primary_key: str
    :param duplication_columns: <description>
    :type duplication_columns: List[str]

    :raises TypeError: Raises type error when input arguments have a invalid type, are missing or are empty.
    :raises TypeError: Raises type error when required columns are missing in the provided delta table.
    """
    if not isinstance(delta_table, DeltaTable):
        raise TypeError("An existing delta table must be specified.")

    if not primary_key:
        raise TypeError("A unique primary key must be specified.")

    if not duplication_columns or len(duplication_columns) == 0:
        raise TypeError("A duplication column must be specified.")

    if primary_key in duplication_columns:
        raise TypeError("Primary key must not be part of the duplication columns.")

    data_frame = delta_table.toDF()

    # Make sure that all the required columns are present in the provided delta table
    append_data_columns = data_frame.columns
    required_columns = [primary_key] + duplication_columns
    for required_column in required_columns:
        if required_column not in append_data_columns:
            raise TypeError(
                f"The base table has these columns {append_data_columns!r}, but these columns are required {required_columns!r}"
            )

    q = []

    duplicate_records = (
        data_frame.withColumn(
            "row_number",
            row_number().over(
                Window().partitionBy(duplication_columns).orderBy(primary_key)
            ),
        )
        .filter(col("row_number") > 1)
        .drop("row_number")
        .distinct()
    )
    for column in required_columns:
        q.append(f"old.{column} = new.{column}")

    q = " AND ".join(q)

    # Remove all the duplicate records
    delta_table.alias("old").merge(
        duplicate_records.alias("new"), q
    ).whenMatchedDelete().execute()


def drop_duplicates(delta_table: DeltaTable, duplication_columns: List[str]) -> None:
    """
    <description>

    :param delta_table: <description>
    :type delta_table: DeltaTable
    :param duplication_columns: <description>
    :type duplication_columns: List[str]

    :raises TypeError: Raises type error when input arguments have a invalid type, are missing or are empty.
    """
    if not isinstance(delta_table, DeltaTable):
        raise TypeError("An existing delta table must be specified.")

    if not duplication_columns or len(duplication_columns) == 0:
        raise TypeError("A duplication column must be specified.")

    data_frame = delta_table.toDF()

    details = delta_table.detail().select("location").collect()[0]

    (
        data_frame.drop_duplicates(duplication_columns)
        .write.format("delta")
        .mode("overwrite")
        .save(details["location"])
    )


def copy_table(
    delta_table: DeltaTable, target_path: str = "", target_table: str = ""
) -> None:
    """
    <description>

    :param delta_table: <description>
    :type delta_table: DeltaTable
    :param target_path: <description>, defaults to empty string.
    :type target_path: str
    :param target_table: <description>, defaults to empty string.
    :type target_table: str

    :raises TypeError: Raises type error when input arguments have a invalid type, are missing or are empty.
    """
    if not isinstance(delta_table, DeltaTable):
        raise TypeError("An existing delta table must be specified.")

    if not target_path and not target_table:
        raise TypeError("Either target_path or target_table must be specified.")

    origin_table = delta_table.toDF()

    details = delta_table.detail().select("partitionColumns", "properties").collect()[0]

    if target_table:
        (
            origin_table.write.format("delta")
            .partitionBy(details["partitionColumns"])
            .options(**details["properties"])
            .saveAsTable(target_table)
        )
    else:
        (
            origin_table.write.format("delta")
            .partitionBy(details["partitionColumns"])
            .options(**details["properties"])
            .save(target_path)
        )


def validate_append(
    delta_table: DeltaTable,
    append_df: DataFrame,
    required_cols: List[str],
    optional_cols: List[str],
) -> None:
    """
    <description>

    :param delta_table: <description>
    :type delta_table: DeltaTable
    :param append_df: <description>
    :type append_df: DataFrame
    :param required_cols: <description>
    :type required_cols: List[str]
    :param optional_cols: <description>
    :type optional_cols: List[str]

    :raises TypeError: Raises type error when input arguments have a invalid type, are missing or are empty.
    :raises TypeError: Raises type error when required columns are missing in the provided delta table.
    :raises TypeError: Raises type error when column in append dataframe is not part of the original delta table..
    """
    if not isinstance(delta_table, DeltaTable):
        raise TypeError("An existing delta table must be specified.")

    if not isinstance(append_df, DataFrame):
        raise TypeError("You must provide a DataFrame that is to be appended.")

    append_data_columns = append_df.columns

    for required_column in required_cols:
        if required_column not in append_data_columns:
            raise TypeError(
                f"The base Delta table has these columns {append_data_columns!r}, but these columns are required {required_cols!r}"
            )

    table_columns = delta_table.toDF().columns

    for column in append_data_columns:
        if column not in table_columns and column not in optional_cols:
            raise TypeError(
                f"The column {column!r} is not part of the current Delta table."
                + " If you want to add the column to the table you must set the optional_cols parameter."
            )

    details = delta_table.detail().select("location").collect()[0]

    (
        append_df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(details["location"])
    )


def append_without_duplicates(
    delta_table: DeltaTable, append_df: DataFrame, p_keys: List[str]
) -> None:
    """
    <description>

    :param delta_table: <description>
    :type delta_table: DeltaTable
    :param append_df: <description>
    :type append_df: DataFrame
    :param p_keys: <description>
    :type p_keys: List[str]

    :raises TypeError: Raises type error when input arguments have a invalid type.
    """
    if not isinstance(delta_table, DeltaTable):
        raise TypeError("An existing delta table must be specified.")

    condition_columns = []
    for column in p_keys:
        condition_columns.append(f"old.{column} = new.{column}")

    condition_columns = " AND ".join(condition_columns)

    deduplicated_append_df = append_df.drop_duplicates(p_keys)

    # Insert records without duplicates
    delta_table.alias("old").merge(
        deduplicated_append_df.alias("new"), condition_columns
    ).whenNotMatchedInsertAll().execute()


def is_composite_key_candidate(delta_table: DeltaTable, cols: List[str]) -> bool:
    """
    <description>

    :param delta_table: <description>
    :type delta_table: DeltaTable
    :param cols: <description>
    :type cols: List[str]

    :raises TypeError: Raises type error when input arguments have a invalid type or are missing.
    :raises TypeError: Raises type error when required columns are not in dataframe columns.

    :returns: <description>
    :rtype: bool
    """
    if not isinstance(delta_table, DeltaTable):
        raise TypeError("An existing delta table must be specified.")

    if not cols or len(cols) == 0:
        raise TypeError("At least one column must be specified.")

    data_frame = delta_table.toDF()

    for required_column in cols:
        if required_column not in data_frame.columns:
            raise TypeError(
                f"The base table has these columns {data_frame.columns!r}, but these columns are required {cols!r}"
            )

    duplicate_records = (
        data_frame.withColumn(
            "amount_of_records",
            count("*").over(Window.partitionBy(cols)),
        )
        .filter(col("amount_of_records") > 1)
        .drop("amount_of_records")
    )

    if len(duplicate_records.take(1)) == 0:
        return True

    return False


def delta_file_sizes(delta_table: DeltaTable) -> Dict[str, int]:
    """
    <description>

    :param delta_table: <description>
    :type delta_table: DeltaTable

    :returns: <description>
    :rtype: Dict[str, int]
    """
    details = delta_table.detail().select("numFiles", "sizeInBytes").collect()[0]
    size_in_bytes, number_of_files = details["sizeInBytes"], details["numFiles"]
    average_file_size_in_bytes = round(size_in_bytes / number_of_files, 0)

    return {
        "size_in_bytes": size_in_bytes,
        "number_of_files": number_of_files,
        "average_file_size_in_bytes": average_file_size_in_bytes,
    }


def show_delta_file_sizes(
    delta_table: DeltaTable, humanize_binary: bool = False
) -> None:
    """
    <description>

    :param delta_table: <description>
    :type delta_table: DeltaTable
    :param humanize_binary: <description>
    :type humanize_binary: bool

    :returns: <description>
    :rtype: None
    """
    details = delta_table.detail().select("numFiles", "sizeInBytes").collect()[0]
    size_in_bytes, number_of_files = details["sizeInBytes"], details["numFiles"]
    average_file_size_in_bytes = round(size_in_bytes / number_of_files, 0)

    if humanize_binary:
        humanized_size_in_bytes = humanize_bytes_binary(size_in_bytes)
        humanized_average_file_size = humanize_bytes_binary(average_file_size_in_bytes)
    else:
        humanized_size_in_bytes = humanize_bytes(size_in_bytes)
        humanized_average_file_size = humanize_bytes(average_file_size_in_bytes)
    humanized_number_of_files = f"{number_of_files:,}"

    print(
        f"The delta table contains {humanized_number_of_files} files with a size of {humanized_size_in_bytes}."
        + f" The average file size is {humanized_average_file_size}"
    )


def humanize_bytes(n: int) -> str:
    """
    <description>

    :param n: <description>
    :type n: int

    :returns: <description>
    :rtype: str
    """
    kilobyte = 1000
    for prefix, k in (
        ("PB", kilobyte**5),
        ("TB", kilobyte**4),
        ("GB", kilobyte**3),
        ("MB", kilobyte**2),
        ("kB", kilobyte**1),
    ):
        if n >= k * 0.9:
            return f"{n / k:.2f} {prefix}"
    return f"{n} B"


def humanize_bytes_binary(n: int) -> str:
    """
    <description>

    :param n: <description>
    :type n: int

    :returns: <description>
    :rtype: str
    """
    kibibyte = 1024
    for prefix, k in (
        ("PB", kibibyte**5),
        ("TB", kibibyte**4),
        ("GB", kibibyte**3),
        ("MB", kibibyte**2),
        ("kB", kibibyte**1),
    ):
        if n >= k * 0.9:
            return f"{n / k:.2f} {prefix}"
    return f"{n} B"


def find_composite_key_candidates(
    df: Union[DeltaTable, DataFrame], exclude_cols: List[str] = None
) -> List:
    """
    <description>

    :param df: <description>
    :type df: DeltaTable or DataFrame
    :param exclude_cols: <description>
    :type exclude_cols: List[str], defaults to None.

    :raises TypeError: Raises type error when no composite key can be found.

    :returns: <description>
    :rtype: List
    """
    if type(df) == DeltaTable:
        df = df.toDF()
    if exclude_cols is None:
        exclude_cols = []
    df_col_excluded = df.drop(*exclude_cols)
    total_cols = len(df_col_excluded.columns)
    total_row_count = df_col_excluded.distinct().count()
    for n in range(1, len(df_col_excluded.columns) + 1):
        for c in combinations(df_col_excluded.columns, n):
            if df_col_excluded.select(*c).distinct().count() == total_row_count:
                if len(df_col_excluded.select(*c).columns) == total_cols:
                    raise ValueError("No composite key candidates could be identified.")
                return list(df_col_excluded.select(*c).columns)


def with_md5_cols(
    df: Union[DeltaTable, DataFrame],
    cols: List[str],
    output_col_name: Optional[str] = None,
) -> DataFrame:
    """
    <description>

    :param df: <description>
    :type df: DeltaTable or DataFrame
    :param cols: <description>
    :type cols: List[str]
    :param output_col_name: <description>
    :type output_col_name: str, defaults to empty string.

    :raises TypeError: Raises type error when no composite key can be found.

    :returns: <description>
    :rtype: DataFrame
    """
    if output_col_name is None:
        output_col_name = "_".join(["md5"] + cols)
    if type(df) == DeltaTable:
        df = df.toDF()
    return df.withColumn(output_col_name, md5(concat_ws("||", *cols)))


def latest_version(delta_table: DeltaTable) -> float:
    """
    <description>

    :param delta_table: <description>
    :type delta_table: DeltaTable

    :returns: <description>
    :rtype: float
    """
    version = delta_table.history().agg(max("version")).collect()[0][0]
    return version


def constraint_append(
    delta_table: DeltaTable, append_df: DataFrame, quarantine_table: DeltaTable
):
    """
    <description>

    :param delta_table: <description>
    :type delta_table: DeltaTable
    :param append_df: <description>
    :type append_df: DataFrame
    :param quarantine_table: <description>
    :type quarantine_table: DeltaTable

    :raises TypeError: Raises type error when input arguments have an invalid type.
    :raises TypeError: Raises type error when delta_table has no constraints.
    """

    if not isinstance(delta_table, DeltaTable):
        raise TypeError("An existing delta table must be specified for delta_table.")

    if not isinstance(append_df, DataFrame):
        raise TypeError("You must provide a DataFrame that is to be appended.")

    if quarantine_table is not None and not isinstance(quarantine_table, DeltaTable):
        raise TypeError(
            "An existing delta table must be specified for quarantine_table."
        )

    properties = delta_table.detail().select("properties").collect()[0]["properties"]
    check_constraints = [
        v for k, v in properties.items() if k.startswith("delta.constraints")
    ]

    # add null checks
    fields = delta_table.toDF().schema.fields
    null_constraints = [
        f"{field.name} is not null" for field in fields if not field.nullable
    ]

    constraints = check_constraints + null_constraints

    if not constraints:
        raise TypeError("There are no constraints present in the target delta table")

    target_details = delta_table.detail().select("location").collect()[0]
    if quarantine_table:
        quarantine_details = quarantine_table.detail().select("location").collect()[0]
        quarantine_df = append_df.filter(
            "not (" + " and ".join([c for c in constraints]) + ")"
        )
        (
            quarantine_df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(quarantine_details["location"])
        )

    filtered_df = append_df.filter(" and ".join([c for c in constraints]))
    (
        filtered_df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(target_details["location"])
    )


def rename_delta_table(
    delta_table: DeltaTable,
    new_table_name: str,
    table_location: str = None,
    databricks: bool = False,
    spark_session: pyspark.sql.SparkSession = None,
) -> None:
    """
    Renames a Delta table to a new name. This function can be used in a Databricks environment or with a
    standalone Spark session.

    Parameters:
    delta_table (DeltaTable): The DeltaTable object representing the table to be renamed.
    new_table_name (str): The new name for the table.
    table_location (str, optional): The file path where the table is stored. Defaults to None.
        If None, the function will attempt to determine the location from the DeltaTable object.
    databricks (bool, optional): A flag indicating whether the function is being run in a Databricks
        environment. Defaults to False. If True, a SparkSession must be provided.
    spark_session (pyspark.sql.SparkSession, optional): The Spark session. Defaults to None.
        Required if `databricks` is set to True.

    Returns:
    None

    Raises:
    TypeError: If the provided `delta_table` is not a DeltaTable object, or if `databricks` is True
        and `spark_session` is None.

    Example Usage:
    >>> rename_delta_table(existing_delta_table, "new_table_name")
    """
    if not isinstance(delta_table, DeltaTable):
        raise TypeError("An existing delta table must be specified for delta_table.")
    if databricks and spark_session is None:
        raise TypeError("A spark session must be specified for databricks.")

    if databricks:
        spark_session.sql(f"ALTER TABLE {delta_table.name} RENAME TO {new_table_name}")
    else:
        delta_table.toDF().write.format("delta").mode("overwrite").saveAsTable(
            new_table_name
        )

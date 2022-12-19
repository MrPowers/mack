from typing import List

from delta import *
import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window


class MackValidationError(ValueError):
    """raise this when there's a Mack validation error"""


def type_2_scd_upsert(path, updates_df, primaryKey, attrColNames):
    return type_2_scd_generic_upsert(
        path,
        updates_df,
        primaryKey,
        attrColNames,
        "is_current",
        "effective_time",
        "end_time",
    )


def type_2_scd_generic_upsert(
        path,
        updates_df,
        primaryKey,
        attrColNames,
        isCurrentColName,
        effectiveTimeColName,
        endTimeColName,
):
    baseTable = DeltaTable.forPath(pyspark.sql.SparkSession.getActiveSession(), path)
    # validate the existing Delta table
    baseColNames = baseTable.toDF().columns
    requiredBaseColNames = [primaryKey] + attrColNames + [isCurrentColName, effectiveTimeColName, endTimeColName]
    if sorted(baseColNames) != sorted(requiredBaseColNames):
        raise MackValidationError(
            f"The base table has these columns '{baseColNames}', but these columns are required '{requiredBaseColNames}'"
        )
    # validate the updates DataFrame
    updatesColNames = updates_df.columns
    requiredUpdatesColNames = [primaryKey] + attrColNames + [effectiveTimeColName]
    if sorted(updatesColNames) != sorted(requiredUpdatesColNames):
        raise MackValidationError(
            f"The updates DataFrame has these columns '{updatesColNames}', but these columns are required '{requiredUpdatesColNames}'"
        )

    # perform the upsert
    updatesAttrs = list(map(lambda attr: f"updates.{attr} <> base.{attr}", attrColNames))
    updatesAttrs = " OR ".join(updatesAttrs)
    stagedUpdatesAttrs = list(map(lambda attr: f"staged_updates.{attr} <> base.{attr}", attrColNames))
    stagedUpdatesAttrs = " OR ".join(stagedUpdatesAttrs)
    stagedPart1 = (
        updates_df.alias("updates")
        .join(baseTable.toDF().alias("base"), primaryKey)
        .where(f"base.{isCurrentColName} = true AND ({updatesAttrs})")
        .selectExpr("NULL as mergeKey", "updates.*")
    )
    stagedPart2 = updates_df.selectExpr(f"{primaryKey} as mergeKey", "*")
    stagedUpdates = stagedPart1.union(stagedPart2)
    thing = {}
    for attr in attrColNames:
        thing[attr] = f"staged_updates.{attr}"
    thing2 = {
        primaryKey: f"staged_updates.{primaryKey}",
        isCurrentColName: "true",
        effectiveTimeColName: f"staged_updates.{effectiveTimeColName}",
        endTimeColName: "null",
    }
    res_thing = {**thing, **thing2}
    res = (
        baseTable.alias("base")
        .merge(
            source=stagedUpdates.alias("staged_updates"),
            condition=pyspark.sql.functions.expr(
                f"base.{primaryKey} = mergeKey AND base.{isCurrentColName} = true AND ({stagedUpdatesAttrs})"
            ),
        )
        .whenMatchedUpdate(
            set={
                isCurrentColName: "false",
                endTimeColName: f"staged_updates.{effectiveTimeColName}",
            }
        )
        .whenNotMatchedInsert(values=res_thing)
        .execute()
    )
    return res


def kill_duplicates(deltaTable, pkey, cols):
    spark = pyspark.sql.SparkSession.getActiveSession()
    colsA = ", ".join(cols)
    deltaTable.toDF().createOrReplaceTempView("temp")
    dfTemp = (
        spark.sql(f"SELECT *, ROW_NUMBER() OVER (PARTITION BY {colsA} ORDER BY {pkey} DESC) rn FROM temp")
    ).filter(F.col('rn') > 1).drop('rn').distinct()

    q = []
    for col in cols:
        q.append(f"main.{col} = nodups.{col}")
    q = " AND ".join(q)

    deltaTable.alias("main").merge(
        dfTemp.alias("nodups"), q
    ).whenMatchedDelete().execute()


def drop_duplicates(delta_table: DeltaTable, primary_key: str, duplication_columns: List[str] = None):
    if not delta_table:
        raise Exception("An existing delta table must be specified.")

    if not primary_key:
        raise Exception("A primary key must be specified.")

    if not duplication_columns:
        duplication_columns = []

    if primary_key in duplication_columns:
        raise Exception("Primary key must not be part of the duplication columns.")

    data_frame = delta_table.toDF()

    # Make sure that all the required columns are present in the provided delta table
    data_frame_columns = data_frame.columns
    required_columns = [primary_key] + duplication_columns
    for required_column in required_columns:
        if required_column not in data_frame_columns:
            raise MackValidationError(
                f"The base table has these columns '{data_frame_columns}', but these columns are required '{required_columns}'"
            )

    q = []

    # Get all the duplicate records
    if len(duplication_columns) > 0:
        duplicate_records = (
            data_frame
            .withColumn("row_number", F.row_number().over(Window().partitionBy(duplication_columns).orderBy(primary_key)))
            .filter(F.col("row_number") > 1)
            .drop("row_number")
            .distinct()
        )
        for column in required_columns:
            q.append(f"old.{column} = new.{column}")

    else:
        duplicate_records = (
            data_frame
            .withColumn("row_number", F.row_number().over(Window().partitionBy(primary_key).orderBy(primary_key)))
            .filter(F.col("row_number") > 1)
            .drop("row_number")
            .distinct()
        )

        for column in duplicate_records.columns + [primary_key]:
            q.append(f"old.{column} = new.{column}")

    q = " AND ".join(q)

    # Remove all the duplicate records
    delta_table.alias("old").merge(
        duplicate_records.alias("new"), q
    ).whenMatchedDelete().execute()


def copy_table(delta_table: DeltaTable, target_path: str = None, target_table: str = None):
    if not delta_table:
        raise Exception("An existing delta table must be specified.")

    if not target_path and not target_table:
        raise Exception("Either target_path or target_table must be specified.")

    origin_table = delta_table.toDF()

    details = (
        delta_table
        .detail()
        .select("partitionColumns", "properties")
        .collect()[0]
    )

    if target_table:
        (
            origin_table
            .write.format("delta")
            .partitionBy(details["partitionColumns"])
            .options(**details["properties"])
            .saveAsTable(target_table)
        )
    else:
        (
            origin_table
            .write
            .format("delta")
            .partitionBy(details["partitionColumns"])
            .options(**details["properties"])
            .save(target_path)
        )

        


def append_without_duplicates(delta_table: DeltaTable, append_data: DataFrame, p_keys: List[str] = None):
    if not delta_table:
        raise Exception("An existing delta table must be specified.")

    condition_columns = []
    for column in p_keys:
        condition_columns.append(f"old.{column} = new.{column}")

    condition_columns = " AND ".join(condition_columns)

    # Insert records without duplicates
    delta_table.alias("old").merge(
        append_data.alias("new"),
        condition_columns
    ).whenNotMatchedInsertAll().execute()

from delta import *
import pyspark
import pyspark.sql.functions as F


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

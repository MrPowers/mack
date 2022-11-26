from delta import *
import pyspark


def type_2_scd_upsert(path, updates_df, primaryKey, attrColNames):
    return type_2_scd_generic_upsert(path, updates_df, primaryKey, attrColNames, "is_current", "effective_time", "end_time")


def type_2_scd_generic_upsert(path, updates_df, primaryKey, attrColNames, isCurrentColName, effectiveTimeColName, endTimeColName):
    baseTable = DeltaTable.forPath(pyspark.sql.SparkSession.getActiveSession(), path)
    # // validate the existing Delta table
    # baseColNames = baseTable.toDF.columns.toSeq
    # requiredBaseColNames = Seq(primaryKey) ++ attrColNames ++ Seq(isCurrentColName, effectiveTimeColName, endTimeColName)
    # // @todo move the validation logic to a separate abstraction
    # if (baseColNames.sorted != requiredBaseColNames.sorted) {
    #     throw JodieValidationError(f"The base table has these columns '$baseColNames', but these columns are required '$requiredBaseColNames'")
    # }
    # // validate the updates DataFrame
    # updatesColNames = updates_df.columns.toSeq
    # requiredUpdatesColNames = Seq(primaryKey) ++ attrColNames ++ Seq(effectiveTimeColName)
    # if (updatesColNames.sorted != requiredUpdatesColNames.sorted) {
    #     throw JodieValidationError(f"The updates DataFrame has these columns '$updatesColNames', but these columns are required '$requiredUpdatesColNames'")
    # }

    # perform the upsert
    # updatesAttrs = attrColNames.map(attr => f"updates.$attr <> base.$attr").mkString(" OR ")
    updatesAttrs = list(map(lambda attr: f"updates.{attr} <> base.{attr}", attrColNames))
    updatesAttrs = " OR ".join(updatesAttrs)
    # stagedUpdatesAttrs = attrColNames.map(attr => f"staged_updates.$attr <> base.$attr").mkString(" OR ")
    stagedUpdatesAttrs = list(map(lambda attr: f"staged_updates.{attr} <> base.{attr}", attrColNames))
    stagedUpdatesAttrs = " OR ".join(stagedUpdatesAttrs)
    stagedPart1 = updates_df.alias("updates").join(baseTable.toDF().alias("base"), primaryKey).where(f"base.{isCurrentColName} = true AND ({updatesAttrs})").selectExpr("NULL as mergeKey", "updates.*")
    # stagedPart1 = updates_df.as("updates").join(baseTable.toDF().as("base"), primaryKey).where(f"base.{isCurrentColName} = true AND ({updatesAttrs})").selectExpr("NULL as mergeKey", "updates.*")
    stagedPart2 = updates_df.selectExpr(f"{primaryKey} as mergeKey", "*")
    stagedUpdates = stagedPart1.union(stagedPart2)
    # thing = attrColNames.map(attr => (attr, f"staged_updates.{attr}")).toMap
    thing = {}
    for attr in attrColNames:
        thing[attr] = f"staged_updates.{attr}"
    thing2 = {
        primaryKey: f"staged_updates.{primaryKey}",
        isCurrentColName: "true",
        effectiveTimeColName: f"staged_updates.{effectiveTimeColName}",
        endTimeColName: "null"
    }
    res_thing = {**thing, **thing2}
    res = (baseTable
        .alias("base")
        .merge(
            source = stagedUpdates.alias("staged_updates"), 
            condition = pyspark.sql.functions.expr(f"base.{primaryKey} = mergeKey AND base.{isCurrentColName} = true AND ({stagedUpdatesAttrs})"))
        .whenMatchedUpdate( 
            set = {isCurrentColName: "false", endTimeColName: f"staged_updates.{effectiveTimeColName}"})
        .whenNotMatchedInsert(values = res_thing)
        .execute())
    return res

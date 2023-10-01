# mack

![![image](https://github.com/MrPowers/mack/workflows/build/badge.svg)](https://github.com/MrPowers/mack/actions/workflows/ci.yml/badge.svg)
![![image](https://github.com/MrPowers/mack/workflows/build/badge.svg)](https://github.com/MrPowers/mack/actions/workflows/black.yml/badge.svg)
![![image](https://github.com/MrPowers/mack/workflows/build/badge.svg)](https://github.com/MrPowers/mack/actions/workflows/ruff.yml/badge.svg)
![PyPI - Downloads](https://img.shields.io/pypi/dm/mack)
[![PyPI version](https://badge.fury.io/py/mack.svg)](https://badge.fury.io/py/mack)

mack provides a variety of helper methods that make it easy for you to perform common Delta Lake operations.

![mack](https://github.com/MrPowers/mack/raw/main/images/mack.jpg)

## Setup

Install mack with `pip install mack`.

Here's an example of how you can perform a Type 2 SCD upsert with a single line of code using Mack:

```python
import mack

mack.type_2_scd_upsert(path, updatesDF, "pkey", ["attr1", "attr2"])
```

## Type 2 SCD Upserts

This library provides an opinionated, conventions over configuration, approach to Type 2 SCD management. Let's look at an example before
covering the conventions required to take advantage of the functionality.

Suppose you have the following SCD table with the `pkey` primary key:

```
+----+-----+-----+----------+-------------------+--------+
|pkey|attr1|attr2|is_current|     effective_time|end_time|
+----+-----+-----+----------+-------------------+--------+
|   1|    A|    A|      true|2019-01-01 00:00:00|    null|
|   2|    B|    B|      true|2019-01-01 00:00:00|    null|
|   4|    D|    D|      true|2019-01-01 00:00:00|    null|
+----+-----+-----+----------+-------------------+--------+
```

You'd like to perform an upsert with this data:

```
+----+-----+-----+-------------------+
|pkey|attr1|attr2|     effective_time|
+----+-----+-----+-------------------+
|   2|    Z| null|2020-01-01 00:00:00| // upsert data
|   3|    C|    C|2020-09-15 00:00:00| // new pkey
+----+-----+-----+-------------------+
```

Here's how to perform the upsert:

```scala
mack.type_2_scd_upsert(delta_table, updatesDF, "pkey", ["attr1", "attr2"])
```

Here's the table after the upsert:

```
+----+-----+-----+----------+-------------------+-------------------+
|pkey|attr1|attr2|is_current|     effective_time|           end_time|
+----+-----+-----+----------+-------------------+-------------------+
|   2|    B|    B|     false|2019-01-01 00:00:00|2020-01-01 00:00:00|
|   4|    D|    D|      true|2019-01-01 00:00:00|               null|
|   1|    A|    A|      true|2019-01-01 00:00:00|               null|
|   3|    C|    C|      true|2020-09-15 00:00:00|               null|
|   2|    Z| null|      true|2020-01-01 00:00:00|               null|
+----+-----+-----+----------+-------------------+-------------------+
```

You can leverage the upsert code if your SCD table meets these requirements:

* Contains a unique primary key column
* Any change in an attribute column triggers an upsert
* SCD logic is exposed via `effective_time`, `end_time` and `is_current` column (you can also use date or version columns for SCD upserts)

## Kill duplicates

The `kill_duplicate` function completely removes all duplicate rows from a Delta table.

Suppose you have the following table:

```
+----+----+----+
|col1|col2|col3|
+----+----+----+
|   1|   A|   A| # duplicate
|   2|   A|   B|
|   3|   A|   A| # duplicate
|   4|   A|   A| # duplicate
|   5|   B|   B| # duplicate
|   6|   D|   D|
|   9|   B|   B| # duplicate
+----+----+----+
```

Run the `kill_duplicates` function:

```python
mack.kill_duplicates(deltaTable, ["col2", "col3"])
```

Here's the ending state of the table:

```
+----+----+----+
|col1|col2|col3|
+----+----+----+
|   2|   A|   B|
|   6|   D|   D|
+----+----+----+
```

## Drop duplicates with Primary Key

The `drop_duplicates_pkey` function removes all but one duplicate row from a Delta table.
**Warning:** You have to provide a primary column that **must contain unique values**, otherwise the method will default to kill the duplicates.
If you can not provide a unique primary key, you can use the `drop_duplicates` method.

Suppose you have the following table:

```
+----+----+----+----+
|col1|col2|col3|col4|
+----+----+----+----+
|   1|   A|   A|   C| # duplicate1
|   2|   A|   B|   C|
|   3|   A|   A|   D| # duplicate1
|   4|   A|   A|   E| # duplicate1
|   5|   B|   B|   C| # duplicate2
|   6|   D|   D|   C|
|   9|   B|   B|   E| # duplicate2
+----+----+----+----+
```

Run the `drop_duplicates` function:

```python
mack.drop_duplicates_pkey(delta_table=deltaTable, primary_key="col1", duplication_columns=["col2", "col3"])
```

Here's the ending state of the table:

```
+----+----+----+----+
|col1|col2|col3|col4|
+----+----+----+----+
|   1|   A|   A|   C|
|   2|   A|   B|   C|
|   5|   B|   B|   C|
|   6|   D|   D|   C|
+----+----+----+----+
```

## Drop duplicates

The `drop_duplicates` function removes all but one duplicate row from a Delta table. It behaves exactly like the `drop_duplicates` DataFrame API.
**Warning:** This method is overwriting the whole table, thus very inefficient. If you can, use the `drop_duplicates_pkey` method instead.

Suppose you have the following table:

```
+----+----+----+----+
|col1|col2|col3|col4|
+----+----+----+----+
|   1|   A|   A|   C| # duplicate
|   1|   A|   A|   C| # duplicate
|   2|   A|   A|   C|
+----+----+----+----+
```

Run the `drop_duplicates` function:

```python
mack.drop_duplicates(delta_table=deltaTable, duplication_columns=["col1"])
```

Here's the ending state of the table:

```
+----+----+----+----+
|col1|col2|col3|col4|
+----+----+----+----+
|   1|   A|   A|   C| # duplicate
|   2|   A|   A|   C| # duplicate
+----+----+----+----+
```

## Copy table

The `copy_table` function copies an existing Delta table.
When you copy a table, it gets recreated at a specified target. This target could be a path or a table in a metastore.
Copying includes:

* Data
* Partitioning
* Table properties

Copying **does not** include the delta log, which means that you will not be able to restore the new table to an old version of the original
table.

Here's how to perform the copy:

```python
mack.copy_table(delta_table=deltaTable, target_path=path)
```

## Validate append

The `validate_append` function provides a mechanism for allowing some columns for schema evolution, but rejecting appends with columns that aren't specificly allowlisted.

Suppose you have the following Delta table:

```
+----+----+----+
|col1|col2|col3|
+----+----+----+
|   2|   b|   B|
|   1|   a|   A|
+----+----+----+
```

Here's a appender function that wraps `validate_append`:

```python
def append_fun(delta_table, append_df):
    mack.validate_append(
        delta_table,
        append_df,
        required_cols=["col1", "col2"],
        optional_cols=["col4"],
    )
```

You can append the following DataFrame that contains the required columns and the optional columns:

```
+----+----+----+
|col1|col2|col4|
+----+----+----+
|   3|   c| cat|
|   4|   d| dog|
+----+----+----+
```

Here's what the Delta table will contain after that data is appended:

```
+----+----+----+----+
|col1|col2|col3|col4|
+----+----+----+----+
|   3|   c|null| cat|
|   4|   d|null| dog|
|   2|   b|   B|null|
|   1|   a|   A|null|
+----+----+----+----+
```

You cannot append the following DataFrame which contains the required columns, but also contains another column (`col5`) that's not specified as an optional column.

```
+----+----+----+
|col1|col2|col5|
+----+----+----+
|   4|   b|   A|
|   5|   y|   C|
|   6|   z|   D|
+----+----+----+
```

Here's the error you'll get when you attempt this write: "TypeError: The column 'col5' is not part of the current Delta table. If you want to add the column to the table you must set the optional_cols parameter."

You also cannot append the following DataFrame which is missing one of the required columns.

```
+----+----+
|col1|col4|
+----+----+
|   4|   A|
|   5|   C|
|   6|   D|
+----+----+
```

Here's the error you'll get: "TypeError: The base Delta table has these columns '['col1', 'col4']', but these columns are required '['col1', 'col2']'."

## Append data without duplicates

The `append_without_duplicates` function helps to append records to a existing Delta table without getting duplicates appended to the
record.

Suppose you have the following Delta table:

```
+----+----+----+
|col1|col2|col3|
+----+----+----+
|   1|   A|   B|
|   2|   C|   D|
|   3|   E|   F|
+----+----+----+
```

Here is data to be appended:

```
+----+----+----+
|col1|col2|col3|
+----+----+----+
|   2|   R|   T| # duplicate col1
|   8|   A|   B|
|   8|   C|   D| # duplicate col1
|  10|   X|   Y|
+----+----+----+
```

Run the `append_without_duplicates` function:

```python
mack.append_without_duplicates(deltaTable, append_df, ["col1"])
```

Here's the ending result:

```

+----+----+----+
|col1|col2|col3|
+----+----+----+
|   1|   A|   B|
|   2|   C|   D|
|   3|   E|   F|
|   8|   A|   B|
|  10|   X|   Y|
+----+----+----+
```

Notice that the duplicate `col1` value was not appended.  If a normal append operation was run, then the Delta table would contain two rows of data with `col1` equal to 2.

## Delta File Sizes

The `delta_file_sizes` function returns a dictionary that contains the total size in bytes, the amount of files and the average file size for a given Delta Table.

Suppose you have the following Delta Table, partitioned by `col1`:

```
+----+----+----+
|col1|col2|col3|
+----+----+----+
|   1|   A|   A|
|   2|   A|   B|
+----+----+----+
```

Running `mack.delta_file_sizes(delta_table)` on that table will return:

```
{"size_in_bytes": 1320,
"number_of_files": 2,
"average_file_size_in_bytes": 660}
```

## Show Delta File Sizes

The `show_delta_file_sizes` function prints the amount of files, the size of the table, and the average file size for a delta table.

Suppose you have the following Delta Table, partitioned by `col1`:

```
+----+----+----+
|col1|col2|col3|
+----+----+----+
|   1|   A|   A|
|   2|   A|   B|
+----+----+----+
```

Running `mack.delta_file_sizes(delta_table)` on that table will print:

`The delta table contains 2 files with a size of 1.32 kB. The average file size is 660.0 B`

## Humanize Bytes

The `humanize_bytes` function formats an integer representing a number of bytes in an easily human readable format.

```python
mack.humanize_bytes(1234567890) # "1.23 GB"
mack.humanize_bytes(1234567890000) # "1.23 TB"
```

It's a lot easier for a human to understand 1.23 GB compared to 1234567890 bytes.

## Is Composite Key Candidate

The `is_composite_key_candidate` function returns a boolean that indicates whether a set of columns are unique and could form a composite key or not.

Suppose you have the following Delta Table:

```
+----+----+----+
|col1|col2|col3|
+----+----+----+
|   1|   A|   A|
|   2|   B|   B|
|   2|   C|   B|
+----+----+----+
```

Running `mack.is_composite_key_candidate(delta_table, ["col1"])` on that table will return `False`.
Running `mack.is_composite_key_candidate(delta_table, ["col1", "col2"])` on that table will return `True`.

## Find Composite Key Candidates in the Delta table

The `find_composite_key_candidates` function helps you find a composite key that uniquely identifies the rows your Delta table.  It returns a list of columns that can be used as a composite key.

Suppose you have the following Delta table:

```
+----+----+----+
|col1|col2|col3|
+----+----+----+
|   1|   a|   z|
|   1|   a|   b|
|   3|   c|   b|
+----+----+----+
```

Running `mack.find_composite_key_candidates(delta_table)` on that table will return `["col1", "col3"]`.

## Append md5 column

The `with_md5_cols` function appends a `md5` hash of specified columns to the DataFrame.  This can be used as a unique key if the selected columns form a composite key.

You can use this function with the columns identified in `find_composite_key_candidates` to append a unique key to the DataFrame.

Suppose you have the following Delta table:

```
+----+----+----+
|col1|col2|col3|
+----+----+----+
|   1|   a|null|
|   2|   b|   b|
|   3|   c|   c|
+----+----+----+
```

Running `mack.with_md5_cols(delta_table, ["col2", "col3"])` on that table will append a `md5_col2_col3` as follows:

```
+----+----+----+--------------------------------+
|col1|col2|col3|md5_col2_col3                   |
+----+----+----+--------------------------------+
|1   |a   |null|0cc175b9c0f1b6a831c399e269772661|
|2   |b   |b   |1eeaac3814eb80cc40efb005cf0b9141|
|3   |c   |c   |4e202f8309e7b00349c70845ab02fce9|
+----+----+----+--------------------------------+
```

## Get Latest Delta Table Version

The `latest_version` function gets the most current Delta
Table version number and returns it.

```python
delta_table = DeltaTable.forPath(spark, path)
mack.latest_version(delta_table)
>> 2
```

## Append data with constraints

The `constraint_append` function helps to append records to an existing Delta table even if there are records in the append dataframe that violate table constraints (both check and not null constraints), these records are appended to an existing quarantine Delta table instead of the target table. If the quarantine Delta table is set to `None`, those records that violate table constraints are simply thrown out.

Suppose you have the following target Delta table with the following schema and constraints:

```
schema:
col1 int not null
col2 string null
col3 string null

check constraints:
col1_constraint: (col1 > 0)
col2_constraint: (col2 != 'Z')

+----+----+----+
|col1|col2|col3|
+----+----+----+
|   1|   A|   B|
|   2|   C|   D|
|   3|   E|   F|
+----+----+----+
```

Suppose you have a quarantine Delta table with the same schema but without the constraints.

Here is data to be appended:

```
+----+----+----+
|col1|col2|col3|
+----+----+----+
|    |   H|   H| # violates col1 not null constraint
|   0|   Z|   Z| # violates both col1_constraint and col2_constraint
|   4|   A|   B|
|   5|   C|   D|
|   6|   E|   F|
|   9|   G|   G|
|  11|   Z|   Z| # violates col2_constraint
+----+----+----+
```

Run the `constraint_append` function:

```python
mack.constraint_append(delta_table, append_df, quarantine_table)
```

Here's the ending result in delta_table:

```

+----+----+----+
|col1|col2|col3|
+----+----+----+
|   1|   A|   B|
|   2|   C|   D|
|   3|   E|   F|
|   4|   A|   B|
|   5|   C|   D|
|   6|   E|   F|
|   9|   G|   G|
+----+----+----+
```

Here's the ending result in quarantine_table:

```

+----+----+----+
|col1|col2|col3|
+----+----+----+
|    |   H|   H|
|   0|   Z|   Z|
|  11|   Z|   Z|
+----+----+----+
```

Notice that the records that violated either of the constraints are appended to the quarantine table all other records are appended to the target table and the append has not failed.  If a normal append operation was run, then it would have failed on the constraint violation. If `quarantine_table` is set to `None`, records that violated either of the constraints are simply thrown out.


## Rename a Delta Table

This function is designed to rename a Delta table. It can operate either within a Databricks environment or with a standalone Spark session. 

## Parameters:

- `delta_table` (`DeltaTable`): An object representing the Delta table to be renamed.
- `new_table_name` (`str`): The new name for the table.
- `table_location` (`str`, optional): The file path where the table is stored. If not provided, the function attempts to deduce the location from the `DeltaTable` object. Defaults to `None`.
- `databricks` (`bool`, optional): A flag indicating the function's operational environment. Set to `True` if running within Databricks, otherwise, `False`. Defaults to `False`.
- `spark_session` (`pyspark.sql.SparkSession`, optional): The Spark session. This is required when `databricks` is set to `True`. Defaults to `None`.

## Returns:

- `None`

## Raises:

- `TypeError`: If the provided `delta_table` is not a DeltaTable object, or if `databricks` is set to `True` and `spark_session` is `None`.

## Example Usage:

```python
rename_delta_table(existing_delta_table, "new_table_name")
```


## Dictionary

We're leveraging the following terminology defined [here](https://www.databasestar.com/database-keys/#:~:text=Natural%20key%3A%20an%20attribute%20that,can%20uniquely%20identify%20a%20row).

* **Natural key:** an attribute that can uniquely identify a row, and exists in the real world.
* **Surrogate key:** an attribute that can uniquely identify a row, and does not exist in the real world.<br>
* **Composite key:** more than one attribute that when combined can uniquely identify a row.
* **Primary key:** the single unique identifier for the row.
* **Candidate key:** an attribute that could be the primary key.
* **Alternate key:** a candidate key that is not the primary key.
* **Unique key:** an attribute that can be unique on the table. Can also be called an alternate key.
* **Foreign key:** an attribute that is used to refer to another record in another table.

## Project maintainers

* Matthew Powers aka [MrPowers](https://github.com/MrPowers)
* Robert Kossendey aka [robertkossendey](https://github.com/robertkossendey)
* Souvik Pratiher aka [souvik-databricks](https://github.com/souvik-databricks)

## Project philosophy

The mack library is designed to make common Delta Lake data tasks easier.

You don't need to use mack of course.  You can write the logic yourself.

If you don't want to add a dependency to your project, you can also easily copy / paste the functions from mack.  The functions in this library are intentionally designed to be easy to copy and paste.

Let's look at some of the reasons you may want to add mack as a dependency.

### Exposing nice public interfaces

The public interface (and only the public interface) is available via the `mack` namespace.

When you run `import mack`, you can access the entirety of the public interface.  No private implementation details are exposed in the `mack` namespace.

### Minimal dependencies

Mack only depends on Spark & Delta Lake.  No other dependencies will be added to Mack.

Spark users leverage a variety of runtimes and it's not always easy to add a dependency.  You can run `pip install mack` and won't have to worry about resolving a lot of dependency conflicts.  You can also Just attach a mack wheel file to a cluster to leverage the project.

### Provide best practices examples for the community

Mack strives to be a good example codebase for the PySpark / Delta Lake community.

There aren't a lot of open source Delta Lake projects.  There are even fewer that use good software engineering practices like CI and unit testing.  You can use mack to help guide your design decisions in proprietary code repos.

### Stable public interfaces and long term support after 1.0 release

Mack reserves the right to make breaking public interface changes before the 1.0 release.  We'll always minimize breaking changes whenever possible.

After the 1.0 release, Mack will stricly follow Semantic Versioning 2.0 and will only make breaking public interface changes in major releases.  Hopefully 1.0 will be the only major release and there won't have to be any breaking changes.

### Code design

Here are some of the code design principles used in Mack:

* We avoid classes whenever possible.  Classes make it harder to copy / paste little chunks of code into notebooks.  It's good to [Stop Writing Classes](https://www.youtube.com/watch?v=o9pEzgHorH0).
* We try to make functions that are easy to copy.  We do this by limiting functions that depend on other functions or classes.  We'd rather nest a single use function in a public interface method than make it separate.
* Develop and then abstract.  All code goes in a single file till the right abstractions become apparent.  We'd rather have a large file than the wrong abstractions.

### Docker Environment
The `Dockerfile` and `docker-compose` files provide a containerized way to run and develop
with `mack`.

- The first time run `docker build --tag=mack .` to build the image.
- To execute the unit tests inside the `Docker` container, run `docker-compose up test`
- To drop into the running `Docker` container to develop, run `docker run -it mack /bin/bash`

## Community

### Blogs

- [Daniel Beach (Confessions of a Data Guy): Simplify Delta Lake Complexity with mack.](https://www.confessionsofadataguy.com/simplify-delta-lake-complexity-with-mack/)
- [Bartosz Konieczny (waitingforcode): Simplified Delta Lake operations with Mack](https://www.waitingforcode.com/delta-lake/simplified-delta-lake-operations-mack/read)

### Videos

- [GeekCoders on YouTube: How I use MACK Library in Delta Lake using Databricks/PySpark](https://www.youtube.com/watch?v=qRR5n6T2N_8)

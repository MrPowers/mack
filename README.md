# mack

![![image](https://github.com/MrPowers/mack/workflows/build/badge.svg)](https://github.com/MrPowers/mack/actions/workflows/ci.yml/badge.svg)
![![image](https://github.com/MrPowers/mack/workflows/build/badge.svg)](https://github.com/MrPowers/mack/actions/workflows/black.yml/badge.svg)
![![image](https://github.com/MrPowers/mack/workflows/build/badge.svg)](https://github.com/MrPowers/mack/actions/workflows/flake8.yml/badge.svg)
![PyPI - Downloads](https://img.shields.io/pypi/dm/mack)
[![PyPI version](https://badge.fury.io/py/mack.svg)](https://badge.fury.io/py/mack)

mack provides a variety of helper methods that make it easy for you to perform common Delta Lake operations.

![mack](https://github.com/MrPowers/mack/blob/main/images/mack.jpg)

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
mack.type_2_scd_upsert(path, updatesDF, "pkey", ["attr1", "attr2"])
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
mack.drop_duplicates_pkey(delta_table=deltaTable, duplication_columns=["col1"])
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

## Find Composite Key

The `find_composite_key` function returns a list of columns that can uniquely identify a row within the data.

Suppose you have the following Delta Table:

```
+------------------------------------+----------------+--------------+--------------+--------------+-------------+---------------+--------------+----------------------------+
|id                                  |dataset         |name          |passed_records|failed_records|status_update|dropped_records|output_records|timestamp                   |
+------------------------------------+----------------+--------------+--------------+--------------+-------------+---------------+--------------+----------------------------+
|c054f1c7-3765-49d6-aa76-debd6e76691c|users_bronze_dlt|correct_schema|60000         |0             |COMPLETED    |0              |1000000       |2021-10-06T14:07:00.000+0000|
|d5d76478-ff24-4bca-aede-c69f31b5b35e|user_silver_dlt |valid_id      |50000         |400           |COMPLETED    |0              |1000000       |2021-10-06T14:07:00.000+0000|
|4b07c459-f414-492a-9f80-640a741c12c6|user_gold_dlt   |valid_income  |60000         |1600          |COMPLETED    |1600           |100000        |2021-10-07T14:02:00.000+0000|
|c054f1c7-3765-49d6-aa76-debd6e76691c|spend_silver_dlt|valid_id      |70000         |500           |COMPLETED    |0              |1000000       |2021-10-08T14:09:00.000+0000|
|c054f1c7-3765-49d6-aa76-debd6e76691c|users_bronze_dlt|correct_schema|70000         |1000          |COMPLETED    |0              |1000000       |2021-10-08T14:09:00.000+0000|
|d5d76478-ff24-4bca-aede-c69f31b5b35e|user_silver_dlt |valid_id      |60000         |1400          |COMPLETED    |0              |1000000       |2021-10-08T14:09:00.000+0000|
|4b07c459-f414-492a-9f80-640a741c12c6|user_gold_dlt   |valid_age     |60000         |1600          |COMPLETED    |1600           |100000        |2021-10-08T14:09:00.000+0000|
|4b07c459-f414-492a-9f80-640a741c12c6|user_gold_dlt   |valid_score   |60000         |1600          |COMPLETED    |1600           |100000        |2021-10-08T14:09:00.000+0000|
+------------------------------------+----------------+--------------+--------------+--------------+-------------+---------------+--------------+----------------------------+
```

Running `mack.find_composite_key(delta_table.toDF(), ['passed_records', 'failed_records', 'status_update', 'dropped_records', 'output_records'])` on that table will return `['id', 'name', 'timestamp']`.

## Get md5 uuid for key columns

The `get_md5` function can be used after using the above-mentioned `find_composite_key` function to generate a `md5 uuid` column based on the identified key columns.

Suppose you have the following Delta Table:

```
+------------------------------------+----------------+--------------+--------------+--------------+-------------+---------------+--------------+----------------------------+
|id                                  |dataset         |name          |passed_records|failed_records|status_update|dropped_records|output_records|timestamp                   |
+------------------------------------+----------------+--------------+--------------+--------------+-------------+---------------+--------------+----------------------------+
|c054f1c7-3765-49d6-aa76-debd6e76691c|users_bronze_dlt|correct_schema|60000         |0             |COMPLETED    |0              |1000000       |2021-10-06T14:07:00.000+0000|
|d5d76478-ff24-4bca-aede-c69f31b5b35e|user_silver_dlt |valid_id      |50000         |400           |COMPLETED    |0              |1000000       |2021-10-06T14:07:00.000+0000|
|4b07c459-f414-492a-9f80-640a741c12c6|user_gold_dlt   |valid_income  |60000         |1600          |COMPLETED    |1600           |100000        |2021-10-07T14:02:00.000+0000|
|c054f1c7-3765-49d6-aa76-debd6e76691c|spend_silver_dlt|valid_id      |70000         |500           |COMPLETED    |0              |1000000       |2021-10-08T14:09:00.000+0000|
|c054f1c7-3765-49d6-aa76-debd6e76691c|users_bronze_dlt|correct_schema|70000         |1000          |COMPLETED    |0              |1000000       |2021-10-08T14:09:00.000+0000|
|d5d76478-ff24-4bca-aede-c69f31b5b35e|user_silver_dlt |valid_id      |60000         |1400          |COMPLETED    |0              |1000000       |2021-10-08T14:09:00.000+0000|
|4b07c459-f414-492a-9f80-640a741c12c6|user_gold_dlt   |valid_age     |60000         |1600          |COMPLETED    |1600           |100000        |2021-10-08T14:09:00.000+0000|
|4b07c459-f414-492a-9f80-640a741c12c6|user_gold_dlt   |valid_score   |60000         |1600          |COMPLETED    |1600           |100000        |2021-10-08T14:09:00.000+0000|
+------------------------------------+----------------+--------------+--------------+--------------+-------------+---------------+--------------+----------------------------+
```

Running `mack.get_md5(delta_table.toDF(),['id', 'name', 'timestamp'])` on that table will return: 

```
+------------------------------------+----------------+--------------+--------------+--------------+-------------+---------------+--------------+----------------------------+--------------------------------+
|id                                  |dataset         |name          |passed_records|failed_records|status_update|dropped_records|output_records|timestamp                   |md5                             |
+------------------------------------+----------------+--------------+--------------+--------------+-------------+---------------+--------------+----------------------------+--------------------------------+
|c054f1c7-3765-49d6-aa76-debd6e76691c|users_bronze_dlt|correct_schema|60000         |0             |COMPLETED    |0              |1000000       |2021-10-06T14:07:00.000+0000|e0d7b4c7c7f36e5b14a14455707868e7|
|d5d76478-ff24-4bca-aede-c69f31b5b35e|user_silver_dlt |valid_id      |50000         |400           |COMPLETED    |0              |1000000       |2021-10-06T14:07:00.000+0000|3d3ec278e10ac253a563612f2536ebb2|
|4b07c459-f414-492a-9f80-640a741c12c6|user_gold_dlt   |valid_income  |60000         |1600          |COMPLETED    |1600           |100000        |2021-10-07T14:02:00.000+0000|b21347fb9cb04fa092f560487dffef4f|
|c054f1c7-3765-49d6-aa76-debd6e76691c|spend_silver_dlt|valid_id      |70000         |500           |COMPLETED    |0              |1000000       |2021-10-08T14:09:00.000+0000|f1177476e14a5a4032c4870304ef7183|
|c054f1c7-3765-49d6-aa76-debd6e76691c|users_bronze_dlt|correct_schema|70000         |1000          |COMPLETED    |0              |1000000       |2021-10-08T14:09:00.000+0000|8cece0c20cfa3a596361f8331336c567|
|d5d76478-ff24-4bca-aede-c69f31b5b35e|user_silver_dlt |valid_id      |60000         |1400          |COMPLETED    |0              |1000000       |2021-10-08T14:09:00.000+0000|e465948ee1bba6e7e90b4c32eddf2867|
|4b07c459-f414-492a-9f80-640a741c12c6|user_gold_dlt   |valid_age     |60000         |1600          |COMPLETED    |1600           |100000        |2021-10-08T14:09:00.000+0000|acbfca92d159c94b4cf2750c2642d591|
|4b07c459-f414-492a-9f80-640a741c12c6|user_gold_dlt   |valid_score   |60000         |1600          |COMPLETED    |1600           |100000        |2021-10-08T14:09:00.000+0000|79d22ad6a8e43dbc45b4681d78ef7f6b|
+------------------------------------+----------------+--------------+--------------+--------------+-------------+---------------+--------------+----------------------------+--------------------------------+
```

## Dictionary

We're leveraging the following terminology defined [here](https://www.databasestar.com/database-keys/#:~:text=Natural%20key%3A%20an%20attribute%20that,can%20uniquely%20identify%20a%20row).

**Natural key:** an attribute that can uniquely identify a row, and exists in the real world.
**Surrogate key:** an attribute that can uniquely identify a row, and does not exist in the real world.<br>
**Composite key:** more than one attribute that when combined can uniquely identify a row.
**Primary key:** the single unique identifier for the row.
**Candidate key:** an attribute that could be the primary key.
**Alternate key:** a candidate key that is not the primary key.
**Unique key:** an attribute that can be unique on the table. Can also be called an alternate key.
**Foreign key:** an attribute that is used to refer to another record in another table.

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

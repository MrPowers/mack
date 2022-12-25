# mack

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

## Drop duplicates

The `drop_duplicates` function removes all but one duplicate row from a Delta table.

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
mack.drop_duplicates(delta_table=deltaTable, primary_key="col1", duplication_columns=["col2", "col3"])
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

Notice that the duplicate `col1` value was not appended. If a normal append operation was run, then the Delta table would contain two rows
of data with `col1` equal to 2.

## Delta File Sizes

The `delta_file_sizes` function returns a dictionary that contains the total size in bytes, the amount of files and the average file size
for a given Delta Table.

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

`{"size_in_bytes": 1320,
"number_of_files": 2,
"average_file_size_in_bites": 660}`

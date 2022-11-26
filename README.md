# mack

mack provides a variety of helper methods that make it easy for you to perform common Delta Lake operations.

For example, you can follow certain table conventions and perform Type 2 SCD upserts with mack using a single line of code.

## Setup

Install mack with `pip install mack`.

Here's how you can perform a Type 2 SCD upsert on a table that follows the right conventions:

```python
mack.type_2_scd_upsert(path, updatesDF, "pkey", ["attr1", "attr2"])
```

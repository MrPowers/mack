# mack

mack provides a variety of helper methods that make it easy for you to perform common Delta Lake operations.

For example, you can follow certain table conventions and perform Type 2 SCD upserts with mack using a single line of code.

## Setup

Install mack with `pip install mack`.

Here's an example of how you can perform a Type 2 SCD upsert with a single line of code using Mack:

```python
import mack

mack.type_2_scd_upsert(path, updatesDF, "pkey", ["attr1", "attr2"])
```

## Type 2 SCD Upserts



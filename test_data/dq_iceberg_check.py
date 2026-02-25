import os
from pyiceberg.catalog import load_catalog

# ---- Iceberg catalog config (explicit, deterministic) ----
os.environ["PYICEBERG_CATALOG__GLUE__TYPE"] = "glue"
os.environ["PYICEBERG_CATALOG__GLUE__URI"] = "glue://"
os.environ["PYICEBERG_CATALOG__GLUE__WAREHOUSE"] = "s3://dataquality-poc-lakehouse/iceberg/"

catalog = load_catalog("glue")

table = catalog.load_table("dq_iceberg_dev.orders")
print(table.schema())

df = table.scan(limit=5).to_arrow().to_pandas()
print(df.head())

from pyiceberg.catalog import load_catalog


class IcebergParser:
    @staticmethod
    def read(
        table_identifier: str,
        columns: list[str] | None = None,
        catalog_name: str = "glue",
        **kwargs,
    ):
        """
        table_identifier examples:
        - iceberg://glue.dq_iceberg_dev.orders
        """

        # Remove URI scheme
        identifier = table_identifier.replace("iceberg://", "")

        # Remove catalog prefix if present
        if identifier.startswith(f"{catalog_name}."):
            identifier = identifier[len(catalog_name) + 1 :]

        # identifier is now: dq_iceberg_dev.orders
        catalog = load_catalog(catalog_name)
        table = catalog.load_table(identifier)

        scan = table.scan()

        if columns:
            scan = scan.select(*columns)

        return scan.to_arrow().to_pandas()

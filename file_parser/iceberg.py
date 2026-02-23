from pyiceberg.catalog import load_catalog
import pandas as pd


class IcebergParser:
    @staticmethod
    def read(
        table_identifier: str,
        columns: list[str] | None = None,
        catalog_name: str = "default",
        **kwargs,
    ) -> pd.DataFrame:
        """
        table_identifier example:
        - glue.db.table
        - warehouse.db.table
        """

        catalog = load_catalog(catalog_name)
        table = catalog.load_table(table_identifier)

        scan = table.scan()

        if columns:
            scan = scan.select(columns)

        arrow_table = scan.to_arrow()
        return arrow_table.to_pandas()

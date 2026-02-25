from pyiceberg.catalog import load_catalog
import pandas as pd
from typing import List, Optional


class IcebergParser:
    @staticmethod
    def read(
        table_identifier: str,
        columns: Optional[List[str]] = None,
        catalog_name: str = "glue",
        **kwargs,
    ) -> pd.DataFrame:
        """
        table_identifier examples:
        - dq_iceberg_dev.orders
        """

        catalog = load_catalog(catalog_name)
        table = catalog.load_table(table_identifier)

        scan = table.scan()

        if columns:
            scan = scan.select(*columns)

        arrow_table = scan.to_arrow()
        return arrow_table.to_pandas()

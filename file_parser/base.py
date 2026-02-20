from abc import ABC, abstractmethod
import pandas as pd

class BaseParser(ABC):

    @staticmethod
    @abstractmethod
    def read(**kwargs) -> pd.DataFrame:
        """
        Read raw file bytes into a pandas DataFrame
        using template-provided instructions
        """
        pass
from db.base import CRUDInterface
from typing import Any, Dict, List

import pandas as pd

class PandasCRUD(CRUDInterface):
    def __init__(self, dataframe: pd.DataFrame):
        self.df = dataframe

    def create(self, data: Dict[str, Any]) -> None:
        self.df = self.df.append(data, ignore_index=True)

    def read(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        result = self.df
        for key, value in query.items():
            result = result[result[key] == value]
        return result.to_dict(orient='records')

    def update(self, query: Dict[str, Any], data: Dict[str, Any]) -> None:
        mask = pd.Series([True] * len(self.df))
        for key, value in query.items():
            mask = mask & (self.df[key] == value)
        self.df.loc[mask, data.keys()] = pd.DataFrame([data])

    def delete(self, query: Dict[str, Any]) -> None:
        mask = pd.Series([True] * len(self.df))
        for key, value in query.items():
            mask = mask & (self.df[key] == value)
        self.df = self.df[~mask]
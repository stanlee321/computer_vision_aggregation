from abc import ABC, abstractmethod
from typing import Any, Dict, List

class CRUDInterface(ABC):
    @abstractmethod
    def create(self, data: Dict[str, Any]) -> None:
        pass

    @abstractmethod
    def read(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def update(self, query: Dict[str, Any], data: Dict[str, Any]) -> None:
        pass

    @abstractmethod
    def delete(self, query: Dict[str, Any]) -> None:
        pass
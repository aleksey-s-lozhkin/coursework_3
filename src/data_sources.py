import json
from abc import ABC, abstractmethod
from typing import Any, Dict, List


class DataSource(ABC):
    """Абстрактный класс источника данных"""

    @abstractmethod
    def get_employers(self) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def get_vacancies(self) -> List[Dict[str, Any]]:
        pass


class JSONDataSource(DataSource):
    """Получение данных из JSON"""

    def __init__(self, filepath: str):
        self.filepath = filepath

    def get_employers(self) -> List[Dict[str, Any]]:
        with open(self.filepath, "r", encoding="utf-8") as f:
            data: Dict[str, Any] = json.load(f)
            employers = data.get("employers", [])
            return employers if isinstance(employers, list) else []

    def get_vacancies(self) -> List[Dict[str, Any]]:
        with open(self.filepath, "r", encoding="utf-8") as f:
            data: Dict[str, Any] = json.load(f)
            vacancies = data.get("vacancies", [])
            return vacancies if isinstance(vacancies, list) else []

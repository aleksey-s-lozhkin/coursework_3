import json
import os
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import requests

from src.vacancy import Vacancy


class ApiClient(ABC):
    """Абстрактный класс для работы с API сервиса с вакансиями"""

    @abstractmethod
    def get_vacancies(self, keyword: str) -> List[Dict]:
        """Метод для получения вакансий от сервиса с вакансиями"""
        pass


class HeadHunterAPIClient(ApiClient):
    """Класс для работы с API сервиса вакансий hh.ru"""

    def __init__(self, base_url: str = 'https://api.hh.ru/vacancies'):
        self.__base_url = base_url

    def __request(self, params: Optional[Dict[str, Any]] = None) -> requests.Response:
        """Приватный метод подключения к API"""

        if params is None:
            params = {}

        response = requests.get(self.__base_url, params=params)

        # Проверка статус-кода
        if response.status_code == 200:
            return response
        elif response.status_code == 400:
            raise requests.exceptions.HTTPError("Неверные параметры запроса")
        elif response.status_code == 403:
            raise requests.exceptions.HTTPError("Доступ запрещен")
        elif response.status_code == 404:
            raise requests.exceptions.HTTPError("Ресурс не найден")
        elif response.status_code == 500:
            raise requests.exceptions.HTTPError("Ошибка сервера")
        else:
            response.raise_for_status()

        return response

    def get_vacancies(self, keyword: str, area: int = 113, per_page: int = 30) -> List[Dict[str, Any]]:
        """Метод для получения вакансий"""

        params = {"text": keyword, "area": area, "per_page": per_page}

        try:
            response = self.__request(params)
            print('Запрос успешно выполнен')
            raw_data = response.json()
            items = raw_data.get("items", [])
            return items if isinstance(items, list) else []
        except requests.exceptions.HTTPError as err:
            print(f"Ошибка HTTP: {err}")
            return []

    def get_vacancies_as_objects(self, keyword: str, area: int = 113, per_page: int = 30) -> List[Vacancy]:
        """Метод для получения вакансий в виде объектов Vacancy"""

        raw_data = self.get_vacancies(keyword, area, per_page)
        return self._convert_vacancy(raw_data)

    @staticmethod
    def _convert_vacancy(raw_data: List[Dict[str, Any]]) -> List[Vacancy]:
        """Преобразование списка словарей в список объектов Vacancy"""

        vacancies = []
        for item in raw_data:
            title = item["name"]
            url = item["alternate_url"]
            salary = item.get("salary", {})  # Гарантируем dict

            # Обработка professional_roles
            professional_roles = item.get("professional_roles", [])
            description = professional_roles[0]["name"] if professional_roles else "Не указано"

            # Обработка snippet
            snippet = item.get("snippet", {})
            responsibility = snippet.get("responsibility", "Не указано")

            # Обработка experience
            experience = item.get("experience", {})
            experience_name = experience.get("name", "Не указан")

            # Создание объекта Vacancy
            vacancy = Vacancy(title, url, salary, description, responsibility, experience_name)
            vacancies.append(vacancy)

        return vacancies

    def save_vacancies_to_json(self, keyword: str, filename: Optional[str] = None, **kwargs) -> bool:
        """Получает вакансии и сразу сохраняет их в JSON файл"""

        # Если имя файла не указано, используем путь по умолчанию
        if filename is None:
            filename = 'data/raw_json.json'

        # Создаем директорию, если она не существует
        os.makedirs(os.path.dirname(filename), exist_ok=True)

        try:
            # Получаем вакансии
            vacancies = self.get_vacancies(keyword, **kwargs)

            # Проверяем, что мы получили данные
            if not vacancies:
                print("Не получено данных для сохранения")
                return False

            with open(filename, 'w', encoding='utf-8') as raw_json:
                json.dump(vacancies, raw_json, ensure_ascii=False, indent=2)
            print(f"Успешно сохранено {len(vacancies)} вакансий в {filename}")
            return True
        except Exception as err:
            print(f"Ошибка при сохранении вакансий: {err}")
            return False

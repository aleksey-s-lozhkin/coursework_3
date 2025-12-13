import requests
import json
import os
import time
from typing import List, Dict, Any, Optional


class HTTPRequestHandler:
    """Класс для обработки HTTP запросов"""

    def __init__(self):
        self.__base_url = 'https://api.hh.ru/'
        self.__headers = {
            'User-Agent': 'CW3/1.0 (aleksey.s.lozhkin@gmail.com.com)',
            'Accept': 'application/json'
        }

    def make_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Выполняет HTTP запрос и возвращает JSON"""
        if params is None:
            params = {}

        url = f'{self.__base_url}{endpoint}'

        try:
            response = requests.get(url, params=params, headers=self.__headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            error_messages = {
                400: "Неверные параметры запроса",
                403: "Доступ запрещен",
                404: "Ресурс не найден",
                500: "Ошибка сервера"
            }
            status_code = e.response.status_code
            message = error_messages.get(status_code, f'HTTP ошибка {status_code}')
            print(f"{message}: {endpoint}")
            raise
        except requests.exceptions.RequestException as e:
            print(f"Ошибка соединения: {e}")
            raise


class DataSaver:
    """Класс для сохранения данных"""

    def __init__(self, default_encoding: str = 'utf-8', indent: int = 2):
        self.__default_encoding = default_encoding
        self.__indent = indent

    def save_to_json(self, data: Any, filename: str) -> bool:
        """Сохраняет данные в JSON файл"""
        try:
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            with open(filename, 'w', encoding=self.__default_encoding) as f:
                json.dump(data, f, ensure_ascii=False, indent=self.__indent)
            return True
        except Exception as e:
            print(f"Ошибка при сохранении данных: {e}")
            return False


class HeadHunterAPIClient:
    """Основной класс для работы с API hh.ru"""

    def __init__(self):
        self._request_handler = HTTPRequestHandler()
        self._data_saver = DataSaver()

    def get_employer_info(self, employer_id: str) -> Optional[Dict[str, Any]]:
        """Получение информации о компании по ID"""
        try:
            data = self._request_handler.make_request(f"employers/{employer_id}")
            print(f'Данные компании {employer_id} успешно получены')
            return data
        except requests.exceptions.HTTPError:
            return None

    def get_employer_vacancies(self, employer_id: str, per_page: int = 100) -> List[Dict[str, Any]]:
        """Получение вакансий компании"""
        vacancies = []
        page = 0
        pages = 1

        while page < pages:
            try:
                params = {
                    "employer_id": employer_id,
                    "page": page,
                    "per_page": per_page
                }

                data = self._request_handler.make_request("vacancies", params)
                vacancies.extend(data.get('items', []))
                pages = data.get('pages', 1)
                page += 1

                time.sleep(0.2)

            except requests.exceptions.HTTPError:
                break

        return vacancies

    def get_companies_data(self, company_ids: List[str]) -> List[Dict[str, Any]]:
        """Получение полных данных по списку компаний"""
        companies_data = []

        for company_id in company_ids:
            print(f"Сбор данных для компании {company_id}...")

            employer_info = self.get_employer_info(company_id)

            if employer_info:
                vacancies = self.get_employer_vacancies(company_id)

                company_data = {
                    'employer': employer_info,
                    'vacancies': vacancies,
                    'vacancies_count': len(vacancies)
                }

                companies_data.append(company_data)
                print(f"Получено {len(vacancies)} вакансий для компании {employer_info.get('name', 'Unknown')}")

            time.sleep(1)

        return companies_data

    def save_companies_data(self, companies_data: List[Dict[str, Any]], filename: str) -> bool:
        """Сохраняет данные компаний"""
        return self._data_saver.save_to_json(companies_data, filename)

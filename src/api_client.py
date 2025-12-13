import requests
import json
import os
import time
from typing import List, Dict, Any, Optional, Union, cast


class HTTPRequestHandler:
    """Класс для обработки HTTP запросов"""

    def __init__(self):
        self.__base_url = 'https://api.hh.ru/'
        self.__headers = {
            'User-Agent': 'CW3/1.0 (aleksey.s.lozhkin@gmail.com.com)',
            'Accept': 'application/json'
        }

    def make_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """Выполняет HTTP запрос и возвращает JSON"""
        if params is None:
            params = {}

        url = f'{self.__base_url}{endpoint}'

        try:
            response = requests.get(url, params=params, headers=self.__headers)
            response.raise_for_status()
            return cast(Union[Dict[str, Any], List[Dict[str, Any]]], response.json())
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
            # Создаем директорию, если она не существует
            directory = os.path.dirname(filename)
            if directory:
                os.makedirs(directory, exist_ok=True)

            # Сохраняем данные в файл
            with open(filename, 'w', encoding=self.__default_encoding) as f:
                json.dump(data, f, ensure_ascii=False, indent=self.__indent)

            # Проверяем, что файл был создан
            if os.path.exists(filename):
                print(f"✓ Файл успешно сохранен: {filename}")
                return True
            else:
                print(f"✗ Не удалось создать файл: {filename}")
                return False

        except (TypeError, ValueError) as e:
            print(f"Ошибка кодирования JSON: {e}")
            return False
        except OSError as e:
            print(f"Ошибка файловой системы: {e}")
            return False
        except Exception as e:
            print(f"Неизвестная ошибка при сохранении данных: {e}")
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
            if isinstance(data, dict):
                print(f'✓ Данные компании {employer_id} успешно получены')
                return cast(Dict[str, Any], data)
            return None
        except requests.exceptions.HTTPError:
            return None

    def get_employer_vacancies(self, employer_id: str, per_page: int = 100) -> List[Dict[str, Any]]:
        """Получение вакансий компании"""
        vacancies: List[Dict[str, Any]] = []
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
                if isinstance(data, dict) and 'items' in data:
                    items = cast(List[Dict[str, Any]], data.get('items', []))
                    vacancies.extend(items)
                    pages = data.get('pages', 1)
                    page += 1
                else:
                    break

                time.sleep(0.2)

            except requests.exceptions.HTTPError:
                break

        return vacancies

    def get_companies_data(self, company_ids: List[str]) -> List[Dict[str, Any]]:
        """Получение полных данных по списку компаний"""
        companies_data: List[Dict[str, Any]] = []

        for company_id in company_ids:
            print(f"Сбор данных для компании {company_id}...")

            employer_info = self.get_employer_info(company_id)

            if employer_info:
                vacancies = self.get_employer_vacancies(company_id)

                company_data: Dict[str, Any] = {
                    'employer': employer_info,
                    'vacancies': vacancies,
                    'vacancies_count': len(vacancies)
                }

                companies_data.append(company_data)
                print(f"Получено {len(vacancies)} вакансий для компании {employer_info.get('name', 'Unknown')}")

            time.sleep(1)

        return companies_data

    def save_companies_data(self, companies_data: List[Dict[str, Any]], filename: str) -> bool:
        """Сохраняет данные компаний в файл"""
        print(f"Сохранение данных в файл: {filename}")

        # Собираем всех работодателей и вакансии
        all_employers: List[Dict[str, Any]] = []
        all_vacancies: List[Dict[str, Any]] = []

        for company in companies_data:
            employer = company.get('employer')
            if employer:
                all_employers.append(employer)

            vacancies = company.get('vacancies', [])
            if vacancies:
                all_vacancies.extend(vacancies)

        # Создаем структуру данных
        data_to_save: Dict[str, Any] = {
            'employers': all_employers,
            'vacancies': all_vacancies,
            'metadata': {
                'total_companies': len(companies_data),
                'total_employers': len(all_employers),
                'total_vacancies': len(all_vacancies),
                'saved_at': time.strftime('%Y-%m-%d %H:%M:%S')
            }
        }

        print(f"Сохранено: {len(all_employers)} работодателей, {len(all_vacancies)} вакансий")

        result: bool = self._data_saver.save_to_json(data_to_save, filename)
        return result


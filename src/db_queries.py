import psycopg2
from src.config import DatabaseConfig
from typing import List, Dict, Any, Optional


class DBManager:
    """Класс для выполнения запросов к базе данных"""

    def __init__(self, config: DatabaseConfig):
        """Инициализация менеджера базы данных"""
        self.config = config
        self._connection = None

    def _get_connection(self):
        """Устанавливает соединение с БД"""
        if self._connection is None or self._connection.closed:
            self._connection = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                database=self.config.dbname,
                user=self.config.user,
                password=self.config.password
            )
        return self._connection

    def _close_connection(self):
        """Закрывает соединение с БД"""
        if self._connection and not self._connection.closed:
            self._connection.close()
            self._connection = None

    def __enter__(self):
        """Контекстный менеджер для автоматического подключения"""
        self._get_connection()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Контекстный менеджер для автоматического закрытия соединения"""
        self._close_connection()

    def get_companies_and_vacancies_count(self) -> List[Dict[str, Any]]:
        """Получает список всех компаний и количество вакансий у каждой компании."""
        query = """
            SELECT 
                e.name as company_name,
                COUNT(v.id) as vacancies_count
            FROM employers e
            LEFT JOIN vacancies v ON e.employer_id = v.employer_id
            GROUP BY e.id, e.name
            ORDER BY vacancies_count DESC
        """

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    columns = [desc[0] for desc in cursor.description]
                    results = []

                    for row in cursor.fetchall():
                        results.append(dict(zip(columns, row)))

                    return results
        except Exception as e:
            print(f"Ошибка при получении списка компаний: {e}")
            return []

    def get_all_vacancies(self) -> List[Dict[str, Any]]:
        """Получает список всех вакансий с указанием названия компании,
        названия вакансии, зарплаты и ссылки на вакансию."""
        query = """
            SELECT 
                e.name as company_name,
                v.name as vacancy_name,
                v.salary_from,
                v.salary_to,
                v.salary_currency,
                v.alternate_url as vacancy_url
            FROM vacancies v
            JOIN employers e ON v.employer_id = e.employer_id
            ORDER BY e.name, v.name
        """

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    columns = [desc[0] for desc in cursor.description]
                    results = []

                    for row in cursor.fetchall():
                        results.append(dict(zip(columns, row)))

                    return results
        except Exception as e:
            print(f"Ошибка при получении всех вакансий: {e}")
            return []

    def get_avg_salary(self) -> Optional[float]:
        """Получает среднюю зарплату по вакансиям."""
        query = """
            SELECT 
                AVG(
                    CASE 
                        WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL 
                            THEN (salary_from + salary_to) / 2.0
                        WHEN salary_from IS NOT NULL THEN salary_from
                        WHEN salary_to IS NOT NULL THEN salary_to
                        ELSE NULL
                    END
                ) as average_salary
            FROM vacancies
            WHERE salary_from IS NOT NULL OR salary_to IS NOT NULL
        """

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    result = cursor.fetchone()

                    if result and result[0]:
                        return float(result[0])
                    return None
        except Exception as e:
            print(f"Ошибка при расчете средней зарплаты: {e}")
            return None

    def get_vacancies_with_higher_salary(self) -> List[Dict[str, Any]]:
        """Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям."""
        avg_salary = self.get_avg_salary()

        if avg_salary is None:
            return []

        query = """
            SELECT 
                e.name as company_name,
                v.name as vacancy_name,
                v.salary_from,
                v.salary_to,
                v.salary_currency,
                v.alternate_url as vacancy_url,
                CASE 
                    WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL 
                        THEN (salary_from + salary_to) / 2.0
                    WHEN salary_from IS NOT NULL THEN salary_from
                    WHEN salary_to IS NOT NULL THEN salary_to
                    ELSE NULL
                END as calculated_salary
            FROM vacancies v
            JOIN employers e ON v.employer_id = e.employer_id
            WHERE (
                (salary_from IS NOT NULL AND salary_from > %s) OR
                (salary_to IS NOT NULL AND salary_to > %s) OR
                (
                    salary_from IS NOT NULL AND salary_to IS NOT NULL AND
                    (salary_from + salary_to) / 2.0 > %s
                )
            )
            ORDER BY calculated_salary DESC
        """

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (avg_salary, avg_salary, avg_salary))
                    columns = [desc[0] for desc in cursor.description]
                    results = []

                    for row in cursor.fetchall():
                        results.append(dict(zip(columns, row)))

                    return results
        except Exception as e:
            print(f"Ошибка при поиске вакансий с высокой зарплатой: {e}")
            return []

    def get_vacancies_with_keyword(self, keyword: str) -> List[Dict[str, Any]]:
        """Получает список всех вакансий, в названии которых содержатся переданные слова."""
        query = """
            SELECT 
                e.name as company_name,
                v.name as vacancy_name,
                v.salary_from,
                v.salary_to,
                v.salary_currency,
                v.alternate_url as vacancy_url,
                v.requirement,
                v.responsibility
            FROM vacancies v
            JOIN employers e ON v.employer_id = e.employer_id
            WHERE v.name ILIKE %s
               OR v.requirement ILIKE %s
               OR v.responsibility ILIKE %s
            ORDER BY e.name, v.name
        """

        search_pattern = f"%{keyword}%"

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (search_pattern, search_pattern, search_pattern))
                    columns = [desc[0] for desc in cursor.description]
                    results = []

                    for row in cursor.fetchall():
                        results.append(dict(zip(columns, row)))

                    return results
        except Exception as e:
            print(f"Ошибка при поиске вакансий по ключевому слову: {e}")
            return []

    def get_vacancies_by_company(self, company_id: int) -> List[Dict[str, Any]]:
        """Получает все вакансии конкретной компании."""
        query = """
            SELECT 
                v.name as vacancy_name,
                v.salary_from,
                v.salary_to,
                v.salary_currency,
                v.alternate_url as vacancy_url,
                v.employment,
                v.experience
            FROM vacancies v
            WHERE v.employer_id = %s
            ORDER BY v.name
        """

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (company_id,))
                    columns = [desc[0] for desc in cursor.description]
                    results = []

                    for row in cursor.fetchall():
                        results.append(dict(zip(columns, row)))

                    return results
        except Exception as e:
            print(f"Ошибка при получении вакансий компании: {e}")
            return []

    def get_top_companies_by_vacancies(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Получает компании с наибольшим количеством вакансий."""
        query = """
            SELECT 
                e.name as company_name,
                COUNT(v.id) as vacancies_count,
                e.alternate_url as company_url
            FROM employers e
            LEFT JOIN vacancies v ON e.employer_id = v.employer_id
            GROUP BY e.id, e.name, e.alternate_url
            ORDER BY vacancies_count DESC
            LIMIT %s
        """

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (limit,))
                    columns = [desc[0] for desc in cursor.description]
                    results = []

                    for row in cursor.fetchall():
                        results.append(dict(zip(columns, row)))

                    return results
        except Exception as e:
            print(f"Ошибка при получении топ-компаний: {e}")
            return []

    def get_salary_statistics(self) -> Dict[str, Any]:
        """Получает статистику по зарплатам."""
        query = """
            SELECT 
                COUNT(*) as total_vacancies,
                COUNT(CASE WHEN salary_from IS NOT NULL OR salary_to IS NOT NULL 
                           THEN 1 END) as vacancies_with_salary,
                AVG(
                    CASE 
                        WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL 
                            THEN (salary_from + salary_to) / 2.0
                        WHEN salary_from IS NOT NULL THEN salary_from
                        WHEN salary_to IS NOT NULL THEN salary_to
                        ELSE NULL
                    END
                ) as avg_salary,
                MIN(
                    CASE 
                        WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL 
                            THEN (salary_from + salary_to) / 2.0
                        WHEN salary_from IS NOT NULL THEN salary_from
                        WHEN salary_to IS NOT NULL THEN salary_to
                        ELSE NULL
                    END
                ) as min_salary,
                MAX(
                    CASE 
                        WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL 
                            THEN (salary_from + salary_to) / 2.0
                        WHEN salary_from IS NOT NULL THEN salary_from
                        WHEN salary_to IS NOT NULL THEN salary_to
                        ELSE NULL
                    END
                ) as max_salary
            FROM vacancies
        """

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    columns = [desc[0] for desc in cursor.description]
                    result = cursor.fetchone()

                    if result:
                        return dict(zip(columns, result))
                    return {}
        except Exception as e:
            print(f"Ошибка при получении статистики зарплат: {e}")
            return {}

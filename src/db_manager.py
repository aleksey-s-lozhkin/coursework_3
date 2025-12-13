import psycopg2
from psycopg2.extensions import connection
from src.config import DatabaseConfig
from typing import Optional, List, Dict, Any
import json


class DatabaseConnection:
    """Подключение к БД"""

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self._connection: Optional[connection] = None

    def connect(self) -> connection:
        """Устанавливает соединение"""
        if not self._connection or self._connection.closed:
            self._connection = psycopg2.connect(**self.config.__dict__)
            self._connection.autocommit = True
        return self._connection

    def disconnect(self):
        """Закрывает соединение"""
        if self._connection and not self._connection.closed:
            self._connection.close()


class DatabaseSchemaManager:
    """Управление структурой БД"""

    def __init__(self, db_connection: connection):
        self.connection = db_connection

    def create_tables(self):
        """Создание всех необходимых таблиц"""
        self.create_employers_table()
        self.create_vacancies_table()

    def create_employers_table(self):
        """Создание таблицы employers"""
        with self.connection.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS employers (
                    id SERIAL PRIMARY KEY,
                    employer_id INTEGER UNIQUE NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    description TEXT,
                    site_url VARCHAR(500),
                    alternate_url VARCHAR(500),
                    logo_urls JSONB,
                    area JSONB,
                    industries JSONB
                )
            """)
        self.connection.commit()
        print("Таблица 'employers' создана или уже существует")

    def create_vacancies_table(self):
        """Создание таблицы vacancies"""
        with self.connection.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS vacancies (
                    id SERIAL PRIMARY KEY,
                    vacancy_id INTEGER UNIQUE NOT NULL,
                    employer_id INTEGER REFERENCES employers(employer_id),
                    name VARCHAR(255) NOT NULL,
                    salary_from INTEGER,
                    salary_to INTEGER,
                    salary_currency VARCHAR(10),
                    salary_gross BOOLEAN,
                    area JSONB,
                    published_at TIMESTAMP,
                    created_at TIMESTAMP,
                    requirement TEXT,
                    responsibility TEXT,
                    employment VARCHAR(100),
                    experience VARCHAR(100),
                    alternate_url VARCHAR(500)
                )
            """)
        self.connection.commit()
        print("Таблица 'vacancies' создана или уже существует")

    def drop_tables(self):
        """Удаление таблиц (для очистки)"""
        with self.connection.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS vacancies CASCADE")
            cursor.execute("DROP TABLE IF EXISTS employers CASCADE")
        self.connection.commit()
        print("Таблицы удалены")


class DatabaseManager:
    """Основной менеджер для работы с БД"""

    def __init__(self, config: DatabaseConfig):
        self.connection_manager = DatabaseConnection(config)
        self.connection = self.connection_manager.connect()
        self.schema_manager = DatabaseSchemaManager(self.connection)

    def insert_employer(self, employer_data: Dict[str, Any]) -> Optional[int]:
        """Добавление работодателя в БД"""
        query = """
            INSERT INTO employers (
                employer_id, name, description, site_url, 
                alternate_url, logo_urls, area, industries
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (employer_id) DO NOTHING
            RETURNING id
        """

        with self.connection.cursor() as cursor:
            try:
                cursor.execute(query, (
                    employer_data.get('id'),
                    employer_data.get('name'),
                    employer_data.get('description'),
                    employer_data.get('site_url'),
                    employer_data.get('alternate_url'),
                    json.dumps(employer_data.get('logo_urls')) if employer_data.get('logo_urls') else None,
                    json.dumps(employer_data.get('area')) if employer_data.get('area') else None,
                    json.dumps(employer_data.get('industries')) if employer_data.get('industries') else None
                ))
                result = cursor.fetchone()
                return result[0] if result else None
            except Exception as e:
                print(f"Ошибка при добавлении работодателя {employer_data.get('id')}: {e}")
                return None

    def insert_vacancy(self, vacancy_data: Dict[str, Any]) -> Optional[int]:
        """Добавление вакансии в БД"""
        query = """
            INSERT INTO vacancies (
                vacancy_id, employer_id, name, salary_from, salary_to,
                salary_currency, salary_gross, area, published_at, created_at,
                requirement, responsibility, employment, experience, alternate_url
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (vacancy_id) DO NOTHING
            RETURNING id
        """

        # Обработка зарплаты
        salary = vacancy_data.get('salary')
        salary_from = salary.get('from') if salary else None
        salary_to = salary.get('to') if salary else None
        salary_currency = salary.get('currency') if salary else None
        salary_gross = salary.get('gross') if salary else None

        # Обработка работодателя
        employer = vacancy_data.get('employer', {})
        employer_id = employer.get('id') if employer else None

        # Обработка дат
        published_at = vacancy_data.get('published_at')
        created_at = vacancy_data.get('created_at')

        # Обработка employment и experience
        employment = vacancy_data.get('employment', {})
        experience = vacancy_data.get('experience', {})

        with self.connection.cursor() as cursor:
            try:
                cursor.execute(query, (
                    vacancy_data.get('id'),
                    employer_id,
                    vacancy_data.get('name'),
                    salary_from,
                    salary_to,
                    salary_currency,
                    salary_gross,
                    json.dumps(vacancy_data.get('area')) if vacancy_data.get('area') else None,
                    published_at,
                    created_at,
                    vacancy_data.get('requirement'),
                    vacancy_data.get('responsibility'),
                    employment.get('name') if employment else None,
                    experience.get('name') if experience else None,
                    vacancy_data.get('alternate_url')
                ))
                result = cursor.fetchone()
                return result[0] if result else None
            except Exception as e:
                print(f"Ошибка при добавлении вакансии {vacancy_data.get('id')}: {e}")
                return None

    def load_data_from_json(self, data_source) -> Dict[str, int]:
        """Загрузка всех данных из источника данных"""
        employers = data_source.get_employers()
        vacancies = data_source.get_vacancies()

        stats = {
            'employers_loaded': 0,
            'vacancies_loaded': 0,
            'employers_total': len(employers),
            'vacancies_total': len(vacancies)
        }

        print(f"Начинаю загрузку {len(employers)} работодателей и {len(vacancies)} вакансий...")

        # Загрузка работодателей
        employer_map = {}  # Для связи employer_id -> id в БД
        for employer in employers:
            db_id = self.insert_employer(employer)
            if db_id:
                employer_map[employer.get('id')] = db_id
                stats['employers_loaded'] += 1
                if stats['employers_loaded'] % 10 == 0:
                    print(f"Загружено работодателей: {stats['employers_loaded']}/{stats['employers_total']}")

        # Загрузка вакансий
        for i, vacancy in enumerate(vacancies, 1):
            self.insert_vacancy(vacancy)
            stats['vacancies_loaded'] += 1
            if i % 50 == 0:
                print(f"Загружено вакансий: {i}/{stats['vacancies_total']}")

        print(f"\nЗагрузка завершена!")
        print(f"Успешно загружено: {stats['employers_loaded']} работодателей и {stats['vacancies_loaded']} вакансий")

        return stats

    def close(self):
        """Закрытие соединения с БД"""
        self.connection_manager.disconnect()
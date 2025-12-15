import re
from unittest.mock import Mock, patch, MagicMock
from src.config import DatabaseConfig
from src.db_manager import (
    DatabaseCreator, DatabaseConnection,
    DatabaseSchemaManager, DatabaseManager
)


@patch('psycopg2.connect')
def test_database_creator_create_database_if_not_exists_new(mock_connect):
    """Тест создания новой базы данных"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    # Симулируем, что БД не существует
    mock_cursor.fetchone.return_value = None

    config = DatabaseConfig(
        dbname='new_db',
        user='test_user',
        password='test_pass'
    )

    creator = DatabaseCreator(config)
    result = creator.create_database_if_not_exists()

    assert result is True
    mock_cursor.execute.assert_any_call(
        "SELECT 1 FROM pg_database WHERE datname = 'new_db'"
    )
    mock_cursor.execute.assert_any_call("CREATE DATABASE new_db")


@patch('psycopg2.connect')
def test_database_creator_create_database_if_not_exists_exists(mock_connect):
    """Тест когда база данных уже существует"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    # Симулируем, что БД уже существует
    mock_cursor.fetchone.return_value = [1]

    config = DatabaseConfig(
        dbname='existing_db',
        user='test_user',
        password='test_pass'
    )

    creator = DatabaseCreator(config)
    result = creator.create_database_if_not_exists()

    assert result is False


@patch('psycopg2.connect')
def test_database_connection_connect(mock_connect):
    """Тест подключения к базе данных"""
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    config = DatabaseConfig(
        dbname='test_db',
        user='test_user',
        password='test_pass'
    )

    connection = DatabaseConnection(config)
    result = connection.connect()

    assert result == mock_conn
    assert result.autocommit is True
    mock_connect.assert_called_once_with(
        user='test_user',
        password='test_pass',
        host='localhost',
        port=5432,
        database='test_db'
    )


@patch('psycopg2.connect')
def test_database_connection_disconnect(mock_connect):
    """Тест отключения от базы данных"""
    mock_conn = MagicMock()
    mock_conn.closed = False
    mock_connect.return_value = mock_conn

    config = DatabaseConfig(
        dbname='test_db',
        user='test_user',
        password='test_pass'
    )

    connection = DatabaseConnection(config)
    connection._connection = mock_conn
    connection.disconnect()

    mock_conn.close.assert_called_once()


def test_database_schema_manager_create_tables():
    """Тест создания таблиц"""
    # Создаем мок соединения
    mock_conn = MagicMock()

    # Создаем мок курсора для контекстного менеджера
    mock_cursor = MagicMock()

    # Настраиваем connection.cursor() чтобы возвращал контекстный менеджер
    # который при входе в контекст возвращает mock_cursor
    cursor_context_manager = MagicMock()
    cursor_context_manager.__enter__ = Mock(return_value=mock_cursor)
    cursor_context_manager.__exit__ = Mock(return_value=None)
    mock_conn.cursor.return_value = cursor_context_manager

    # Создаем менеджер схемы
    schema_manager = DatabaseSchemaManager(mock_conn)

    # Вызываем метод создания таблиц
    schema_manager.create_tables()

    # Проверяем, что были вызваны методы создания отдельных таблиц
    # (они вызываются внутри create_tables)
    assert mock_cursor.execute.call_count == 2  # По одному для каждой таблицы
    assert mock_conn.commit.call_count == 2  # По одному после каждой таблицы


def test_database_schema_manager_create_employers_table():
    """Тест создания таблицы employers"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()

    # Настраиваем контекстный менеджер для курсора
    cursor_context_manager = MagicMock()
    cursor_context_manager.__enter__ = Mock(return_value=mock_cursor)
    cursor_context_manager.__exit__ = Mock(return_value=None)
    mock_conn.cursor.return_value = cursor_context_manager

    schema_manager = DatabaseSchemaManager(mock_conn)
    schema_manager.create_employers_table()

    # Проверяем, что execute был вызван с правильным запросом
    assert mock_cursor.execute.call_count == 1
    call_args = mock_cursor.execute.call_args[0][0]
    assert "CREATE TABLE IF NOT EXISTS employers" in call_args
    assert "employer_id INTEGER UNIQUE NOT NULL" in call_args
    mock_conn.commit.assert_called_once()


# Вспомогательная функция для нормализации SQL
def normalize_sql(sql_query):
    """Нормализует SQL запрос для сравнения (удаляет лишние пробелы и переводы строк)"""
    # Заменяем множественные пробелы и переводы строк на один пробел
    return re.sub(r'\s+', ' ', sql_query).strip()


def test_database_schema_manager_create_vacancies_table_normalized():
    """Тест создания таблицы vacancies с нормализацией SQL"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()

    # Настраиваем контекстный менеджер для курсора
    cursor_context_manager = MagicMock()
    cursor_context_manager.__enter__ = Mock(return_value=mock_cursor)
    cursor_context_manager.__exit__ = Mock(return_value=None)
    mock_conn.cursor.return_value = cursor_context_manager

    schema_manager = DatabaseSchemaManager(mock_conn)
    schema_manager.create_vacancies_table()

    # Проверяем, что execute был вызван
    assert mock_cursor.execute.call_count == 1
    call_args = mock_cursor.execute.call_args[0][0]

    # Нормализуем SQL запрос
    normalized_sql = normalize_sql(call_args)

    # Проверяем ключевые части
    assert "CREATE TABLE IF NOT EXISTS vacancies" in normalized_sql
    assert "vacancy_id INTEGER UNIQUE NOT NULL" in normalized_sql
    assert "FOREIGN KEY (employer_id) REFERENCES employers(employer_id)" in normalized_sql
    assert "ON DELETE CASCADE" in normalized_sql

    mock_conn.commit.assert_called_once()


@patch.object(DatabaseCreator, 'create_database_if_not_exists')
@patch.object(DatabaseConnection, 'connect')
@patch.object(DatabaseSchemaManager, 'create_tables')
def test_database_manager_init(mock_create_tables, mock_connect, mock_create_db):
    """Тест инициализации DatabaseManager"""
    mock_create_db.return_value = True
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    config = DatabaseConfig(
        dbname='test_db',
        user='test_user',
        password='test_pass'
    )

    db_manager = DatabaseManager(config)

    assert db_manager.config == config
    assert db_manager.connection == mock_conn
    mock_create_db.assert_called_once()
    mock_connect.assert_called_once()
    mock_create_tables.assert_called_once()


@patch('src.db_manager.psycopg2')
def test_database_manager_insert_employer(mock_psycopg2):
    """Тест добавления работодателя"""
    # Мокаем соединение и курсор
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_psycopg2.connect.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
    mock_cursor.fetchone.return_value = [1]

    config = DatabaseConfig(
        dbname='test_db',
        user='test_user',
        password='test_pass'
    )

    # Используем патч для инициализации DatabaseManager
    with patch.object(DatabaseManager, '__init__', lambda self, cfg: None):
        db_manager = DatabaseManager(config)
        db_manager.config = config
        db_manager.connection = mock_conn

    employer_data = {
        'id': 123,
        'name': 'Test Company',
        'description': 'Test description',
        'site_url': 'http://test.com',
        'alternate_url': 'http://hh.ru/company/123',
        'logo_urls': {'original': 'http://logo.com/logo.png'},
        'area': {'id': '1', 'name': 'Moscow'},
        'industries': [{'id': '1', 'name': 'IT'}]
    }

    result = db_manager.insert_employer(employer_data)

    assert result == 1
    assert mock_cursor.execute.call_count == 1


@patch('src.db_manager.psycopg2')
def test_database_manager_insert_employer_conflict(mock_psycopg2):
    """Тест добавления работодателя с конфликтом (дубликат)"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_psycopg2.connect.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
    mock_cursor.fetchone.return_value = None  # Нет возвращаемого значения при конфликте

    config = DatabaseConfig(
        dbname='test_db',
        user='test_user',
        password='test_pass'
    )

    with patch.object(DatabaseManager, '__init__', lambda self, cfg: None):
        db_manager = DatabaseManager(config)
        db_manager.config = config
        db_manager.connection = mock_conn

    employer_data = {
        'id': 123,
        'name': 'Test Company'
    }

    result = db_manager.insert_employer(employer_data)

    assert result is None
    assert mock_cursor.execute.call_count == 1


@patch('src.db_manager.psycopg2')
def test_database_manager_insert_vacancy(mock_psycopg2):
    """Тест добавления вакансии"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_psycopg2.connect.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
    mock_cursor.fetchone.return_value = [42]

    config = DatabaseConfig(
        dbname='test_db',
        user='test_user',
        password='test_pass'
    )

    with patch.object(DatabaseManager, '__init__', lambda self, cfg: None):
        db_manager = DatabaseManager(config)
        db_manager.config = config
        db_manager.connection = mock_conn

    vacancy_data = {
        'id': 456,
        'name': 'Test Vacancy',
        'salary': {'from': 100000, 'to': 150000, 'currency': 'RUB', 'gross': True},
        'employer': {'id': 123},
        'area': {'id': '1', 'name': 'Moscow'},
        'published_at': '2024-01-01T10:00:00',
        'created_at': '2024-01-01T09:00:00',
        'requirement': 'Test requirement',
        'responsibility': 'Test responsibility',
        'employment': {'name': 'full'},
        'experience': {'name': '1-3 years'},
        'alternate_url': 'http://hh.ru/vacancy/456'
    }

    result = db_manager.insert_vacancy(vacancy_data)

    assert result == 42
    assert mock_cursor.execute.call_count == 1


@patch.object(DatabaseManager, 'insert_employer')
@patch.object(DatabaseManager, 'insert_vacancy')
def test_database_manager_load_data_from_json(mock_insert_vacancy, mock_insert_employer):
    """Тест загрузки данных из JSON"""
    mock_insert_employer.return_value = 1
    mock_insert_vacancy.return_value = 1

    config = DatabaseConfig(
        dbname='test_db',
        user='test_user',
        password='test_pass'
    )

    # Используем патч для DatabaseManager, чтобы не создавать реальное подключение
    with patch.object(DatabaseManager, '__init__', lambda self, cfg: None):
        db_manager = DatabaseManager(config)
        db_manager.config = config
        db_manager.connection = None

    # Мокаем data_source
    mock_data_source = MagicMock()
    employers = [
        {'id': 1, 'name': 'Company A'},
        {'id': 2, 'name': 'Company B'}
    ]
    vacancies = [
        {'id': 1, 'name': 'Vacancy 1', 'employer': {'id': 1}},
        {'id': 2, 'name': 'Vacancy 2', 'employer': {'id': 2}},
        {'id': 3, 'name': 'Vacancy 3', 'employer': {'id': 1}}
    ]
    mock_data_source.get_employers.return_value = employers
    mock_data_source.get_vacancies.return_value = vacancies

    stats = db_manager.load_data_from_json(mock_data_source)

    assert stats['employers_total'] == 2
    assert stats['vacancies_total'] == 3
    assert stats['employers_loaded'] == 2
    assert stats['vacancies_loaded'] == 3

    # Проверяем, что методы были вызваны правильное количество раз
    assert mock_insert_employer.call_count == 2
    assert mock_insert_vacancy.call_count == 3

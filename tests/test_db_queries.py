from unittest.mock import Mock, patch, MagicMock
from src.config import DatabaseConfig
from src.db_queries import DBManager


def test_db_manager_init():
    """Тест инициализации DBManager"""
    config = DatabaseConfig(
        dbname='test_db',
        user='test_user',
        password='test_pass'
    )

    db_manager = DBManager(config)
    assert db_manager.config == config
    assert db_manager._connection is None


@patch('psycopg2.connect')
def test_db_manager_get_connection(mock_connect):
    """Тест получения соединения с БД"""
    mock_conn = MagicMock()
    mock_conn.closed = False
    mock_connect.return_value = mock_conn

    config = DatabaseConfig(
        dbname='test_db',
        user='test_user',
        password='test_pass'
    )

    db_manager = DBManager(config)
    connection = db_manager._get_connection()

    assert connection == mock_conn
    mock_connect.assert_called_once_with(
        host='localhost',
        port=5432,
        database='test_db',
        user='test_user',
        password='test_pass'
    )


@patch('psycopg2.connect')
def test_db_manager_close_connection(mock_connect):
    """Тест закрытия соединения с БД"""
    mock_conn = MagicMock()
    mock_conn.closed = False
    mock_connect.return_value = mock_conn

    config = DatabaseConfig(
        dbname='test_db',
        user='test_user',
        password='test_pass'
    )

    db_manager = DBManager(config)
    db_manager._connection = mock_conn
    db_manager._close_connection()

    mock_conn.close.assert_called_once()


@patch('psycopg2.connect')
def test_db_manager_context_manager(mock_connect):
    """Тест использования DBManager как контекстного менеджера"""
    mock_conn = MagicMock()
    mock_conn.closed = False
    mock_connect.return_value = mock_conn

    config = DatabaseConfig(
        dbname='test_db',
        user='test_user',
        password='test_pass'
    )

    # Используем DBManager как контекстный менеджер
    with DBManager(config) as db_manager:
        assert db_manager is not None
        assert db_manager.config == config

    # Проверяем, что соединение было закрыто
    mock_conn.close.assert_called_once()


@patch('psycopg2.connect')
def test_db_manager_enter_exit(mock_connect):
    """Тест методов __enter__ и __exit__"""
    mock_conn = MagicMock()
    mock_conn.closed = False
    mock_connect.return_value = mock_conn

    config = DatabaseConfig(
        dbname='test_db',
        user='test_user',
        password='test_pass'
    )

    db_manager = DBManager(config)

    # Проверяем __enter__
    db_manager._get_connection = Mock(return_value=mock_conn)
    context_manager = db_manager.__enter__()
    assert context_manager == db_manager

    # Проверяем __exit__
    db_manager._close_connection = Mock()
    db_manager.__exit__(None, None, None)
    db_manager._close_connection.assert_called_once()


@patch('psycopg2.connect')
def test_get_vacancies_by_company(mock_connect):
    """Тест получения вакансий конкретной компании"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_conn

    # Настраиваем контекстные менеджеры
    mock_conn.__enter__ = Mock(return_value=mock_conn)
    mock_conn.__exit__ = Mock(return_value=None)
    mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)

    mock_cursor.description = [
        ('vacancy_name',), ('salary_from',), ('salary_to',),
        ('salary_currency',), ('vacancy_url',), ('employment',), ('experience',)
    ]
    mock_cursor.fetchall.return_value = [
        ('Developer', 100000, 150000, 'RUB', 'http://example.com', 'full', '1-3 years'),
        ('Manager', 80000, 120000, 'RUB', 'http://example2.com', 'full', '3-6 years')
    ]

    config = DatabaseConfig(
        dbname='test_db',
        user='test_user',
        password='test_pass'
    )

    db_manager = DBManager(config)
    db_manager._get_connection = Mock(return_value=mock_conn)

    result = db_manager.get_vacancies_by_company(123)

    assert len(result) == 2
    assert result[0]['vacancy_name'] == 'Developer'
    assert result[1]['vacancy_name'] == 'Manager'

    # Проверяем, что execute был вызван с правильными аргументами
    mock_cursor.execute.assert_called_once()
    call_args = mock_cursor.execute.call_args
    assert "WHERE v.employer_id = %s" in call_args[0][0]
    assert call_args[0][1] == (123,)


@patch('psycopg2.connect')
def test_get_companies_and_vacancies_count(mock_connect):
    """Тест получения компаний и количества вакансий"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_conn

    mock_conn.__enter__ = Mock(return_value=mock_conn)
    mock_conn.__exit__ = Mock(return_value=None)
    mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)

    mock_cursor.description = [('company_name',), ('vacancies_count',)]
    mock_cursor.fetchall.return_value = [
        ('Company A', 10),
        ('Company B', 5)
    ]

    config = DatabaseConfig(
        dbname='test_db',
        user='test_user',
        password='test_pass'
    )

    db_manager = DBManager(config)
    db_manager._get_connection = Mock(return_value=mock_conn)

    result = db_manager.get_companies_and_vacancies_count()

    assert len(result) == 2
    assert result[0]['company_name'] == 'Company A'
    assert result[0]['vacancies_count'] == 10
    assert result[1]['company_name'] == 'Company B'
    assert result[1]['vacancies_count'] == 5


@patch('psycopg2.connect')
def test_get_all_vacancies(mock_connect):
    """Тест получения всех вакансий"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_conn

    mock_conn.__enter__ = Mock(return_value=mock_conn)
    mock_conn.__exit__ = Mock(return_value=None)
    mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)

    mock_cursor.description = [
        ('company_name',), ('vacancy_name',), ('salary_from',),
        ('salary_to',), ('salary_currency',), ('vacancy_url',)
    ]
    mock_cursor.fetchall.return_value = [
        ('Company A', 'Developer', 100000, 150000, 'RUB', 'http://example.com'),
        ('Company B', 'Manager', 80000, 120000, 'RUB', 'http://example2.com')
    ]

    config = DatabaseConfig(
        dbname='test_db',
        user='test_user',
        password='test_pass'
    )

    db_manager = DBManager(config)
    db_manager._get_connection = Mock(return_value=mock_conn)

    result = db_manager.get_all_vacancies()

    assert len(result) == 2
    assert result[0]['company_name'] == 'Company A'
    assert result[0]['vacancy_name'] == 'Developer'
    assert result[1]['vacancy_name'] == 'Manager'


@patch('psycopg2.connect')
def test_get_avg_salary(mock_connect):
    """Тест получения средней зарплаты"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_conn

    mock_conn.__enter__ = Mock(return_value=mock_conn)
    mock_conn.__exit__ = Mock(return_value=None)
    mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)

    mock_cursor.fetchone.return_value = (125000.5,)

    config = DatabaseConfig(
        dbname='test_db',
        user='test_user',
        password='test_pass'
    )

    db_manager = DBManager(config)
    db_manager._get_connection = Mock(return_value=mock_conn)

    result = db_manager.get_avg_salary()

    assert result == 125000.5


@patch('psycopg2.connect')
def test_get_avg_salary_none(mock_connect):
    """Тест получения средней зарплаты (нет данных)"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_conn

    mock_conn.__enter__ = Mock(return_value=mock_conn)
    mock_conn.__exit__ = Mock(return_value=None)
    mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)

    mock_cursor.fetchone.return_value = (None,)

    config = DatabaseConfig(
        dbname='test_db',
        user='test_user',
        password='test_pass'
    )

    db_manager = DBManager(config)
    db_manager._get_connection = Mock(return_value=mock_conn)

    result = db_manager.get_avg_salary()

    assert result is None


@patch('psycopg2.connect')
def test_get_vacancies_with_higher_salary(mock_connect):
    """Тест получения вакансий с зарплатой выше средней"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_conn

    mock_conn.__enter__ = Mock(return_value=mock_conn)
    mock_conn.__exit__ = Mock(return_value=None)
    mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)

    mock_cursor.description = [
        ('company_name',), ('vacancy_name',), ('salary_from',),
        ('salary_to',), ('salary_currency',), ('vacancy_url',), ('calculated_salary',)
    ]
    mock_cursor.fetchall.return_value = [
        ('Company A', 'Senior Developer', 200000, 250000, 'RUB', 'http://example.com', 225000.0),
        ('Company B', 'Lead Manager', 180000, 220000, 'RUB', 'http://example2.com', 200000.0)
    ]

    config = DatabaseConfig(
        dbname='test_db',
        user='test_user',
        password='test_pass'
    )

    db_manager = DBManager(config)
    db_manager._get_connection = Mock(return_value=mock_conn)
    db_manager.get_avg_salary = Mock(return_value=150000.0)

    result = db_manager.get_vacancies_with_higher_salary()

    assert len(result) == 2
    assert result[0]['vacancy_name'] == 'Senior Developer'
    assert result[0]['calculated_salary'] == 225000.0


@patch('psycopg2.connect')
def test_get_vacancies_with_keyword(mock_connect):
    """Тест поиска вакансий по ключевому слову"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_conn

    mock_conn.__enter__ = Mock(return_value=mock_conn)
    mock_conn.__exit__ = Mock(return_value=None)
    mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)

    mock_cursor.description = [
        ('company_name',), ('vacancy_name',), ('salary_from',),
        ('salary_to',), ('salary_currency',), ('vacancy_url',),
        ('requirement',), ('responsibility',)
    ]
    mock_cursor.fetchall.return_value = [
        ('Company A', 'Python Developer', 100000, 150000, 'RUB',
         'http://example.com', 'Python, Django', 'Разработка backend'),
        ('Company B', 'Data Scientist', 120000, 180000, 'RUB',
         'http://example2.com', 'Python, ML', 'Анализ данных')
    ]

    config = DatabaseConfig(
        dbname='test_db',
        user='test_user',
        password='test_pass'
    )

    db_manager = DBManager(config)
    db_manager._get_connection = Mock(return_value=mock_conn)

    result = db_manager.get_vacancies_with_keyword('Python')

    assert len(result) == 2
    assert 'Python' in result[0]['vacancy_name'] or 'Python' in result[0]['requirement']


@patch('psycopg2.connect')
def test_get_top_companies_by_vacancies(mock_connect):
    """Тест получения топ компаний по количеству вакансий"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_conn

    mock_conn.__enter__ = Mock(return_value=mock_conn)
    mock_conn.__exit__ = Mock(return_value=None)
    mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)

    mock_cursor.description = [
        ('company_name',), ('vacancies_count',), ('company_url',)
    ]
    mock_cursor.fetchall.return_value = [
        ('Company A', 50, 'http://company-a.com'),
        ('Company B', 30, 'http://company-b.com'),
        ('Company C', 20, 'http://company-c.com')
    ]

    config = DatabaseConfig(
        dbname='test_db',
        user='test_user',
        password='test_pass'
    )

    db_manager = DBManager(config)
    db_manager._get_connection = Mock(return_value=mock_conn)

    result = db_manager.get_top_companies_by_vacancies(3)

    assert len(result) == 3
    assert result[0]['company_name'] == 'Company A'
    assert result[0]['vacancies_count'] == 50
    assert result[2]['company_name'] == 'Company C'


@patch('psycopg2.connect')
def test_get_salary_statistics(mock_connect):
    """Тест получения статистики зарплат"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_conn

    mock_conn.__enter__ = Mock(return_value=mock_conn)
    mock_conn.__exit__ = Mock(return_value=None)
    mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)

    mock_cursor.description = [
        ('total_vacancies',), ('vacancies_with_salary',),
        ('avg_salary',), ('min_salary',), ('max_salary',)
    ]
    mock_cursor.fetchone.return_value = (100, 80, 120000.5, 50000.0, 250000.0)

    config = DatabaseConfig(
        dbname='test_db',
        user='test_user',
        password='test_pass'
    )

    db_manager = DBManager(config)
    db_manager._get_connection = Mock(return_value=mock_conn)

    result = db_manager.get_salary_statistics()

    assert result['total_vacancies'] == 100
    assert result['vacancies_with_salary'] == 80
    assert result['avg_salary'] == 120000.5
    assert result['min_salary'] == 50000.0
    assert result['max_salary'] == 250000.0


@patch('psycopg2.connect')
def test_db_methods_error_handling(mock_connect):
    """Тест обработки ошибок в методах DBManager"""
    mock_connect.side_effect = Exception("Connection failed")

    config = DatabaseConfig(
        dbname='test_db',
        user='test_user',
        password='test_pass'
    )

    db_manager = DBManager(config)

    # Тестируем все методы на возврат пустых значений при ошибке
    assert db_manager.get_companies_and_vacancies_count() == []
    assert db_manager.get_all_vacancies() == []
    assert db_manager.get_avg_salary() is None
    assert db_manager.get_vacancies_with_higher_salary() == []
    assert db_manager.get_vacancies_with_keyword('test') == []
    assert db_manager.get_vacancies_by_company(1) == []
    assert db_manager.get_top_companies_by_vacancies(10) == []
    assert db_manager.get_salary_statistics() == {}

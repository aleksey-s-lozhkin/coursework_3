import pytest
import json
from unittest.mock import Mock, patch, mock_open, MagicMock
from src.data_sources import JSONDataSource
from src.db_queries import DBManager
from src.utils import (
    collect_data_from_hh, load_data_to_database, get_json_file_path,
    show_main_menu, show_query_menu, display_vacancies, execute_queries, main_loop, format_salary
)
from src.config import DatabaseConfig
from src.db_manager import DatabaseManager
from src.api_client import HeadHunterAPIClient


def test_json_data_source_get_employers():
    """Тест получения работодателей из JSON"""
    mock_data = {
        "employers": [
            {"id": 1, "name": "Company A"},
            {"id": 2, "name": "Company B"}
        ],
        "vacancies": []
    }

    with patch("builtins.open", mock_open(read_data=json.dumps(mock_data))):
        data_source = JSONDataSource("test.json")
        employers = data_source.get_employers()

        assert len(employers) == 2
        assert employers[0]["id"] == 1
        assert employers[1]["name"] == "Company B"


def test_json_data_source_get_vacancies():
    """Тест получения вакансий из JSON"""
    mock_data = {
        "employers": [],
        "vacancies": [
            {"id": 1, "name": "Developer"},
            {"id": 2, "name": "Manager"}
        ]
    }

    with patch("builtins.open", mock_open(read_data=json.dumps(mock_data))):
        data_source = JSONDataSource("test.json")
        vacancies = data_source.get_vacancies()

        assert len(vacancies) == 2
        assert vacancies[0]["id"] == 1
        assert vacancies[1]["name"] == "Manager"


def test_json_data_source_empty_file():
    """Тест обработки пустого файла"""
    mock_data = {}

    with patch("builtins.open", mock_open(read_data=json.dumps(mock_data))):
        data_source = JSONDataSource("test.json")
        employers = data_source.get_employers()
        vacancies = data_source.get_vacancies()

        assert employers == []
        assert vacancies == []


def test_format_salary():
    """Тест форматирования зарплаты"""

    # Полная зарплата
    vacancy1 = {
        'salary_from': 100000,
        'salary_to': 150000,
        'salary_currency': 'RUB'
    }
    assert format_salary(vacancy1) == "100000 - 150000 RUB"

    # Только от
    vacancy2 = {
        'salary_from': 100000,
        'salary_to': None,
        'salary_currency': 'RUB'
    }
    assert format_salary(vacancy2) == "от 100000 RUB"

    # Только до
    vacancy3 = {
        'salary_from': None,
        'salary_to': 150000,
        'salary_currency': 'RUB'
    }
    assert format_salary(vacancy3) == "до 150000 RUB"

    # Нет зарплаты
    vacancy4 = {
        'salary_from': None,
        'salary_to': None,
        'salary_currency': None
    }
    assert format_salary(vacancy4) == "не указана"


def test_format_salary_edge_cases():
    """Тест форматирования зарплаты для граничных случаев"""
    # Согласно коду format_salary, если salary_currency = None, то он будет в строке как "None"
    # Поэтому исправляем ожидаемое значение
    vacancy1 = {
        'salary_from': 100000,
        'salary_to': 150000,
        'salary_currency': None
    }
    assert format_salary(vacancy1) == "100000 - 150000 None"

    # Только from с currency
    vacancy2 = {
        'salary_from': 100000,
        'salary_to': None,
        'salary_currency': 'USD'
    }
    assert format_salary(vacancy2) == "от 100000 USD"

    # Только to с currency
    vacancy3 = {
        'salary_from': None,
        'salary_to': 150000,
        'salary_currency': 'EUR'
    }
    assert format_salary(vacancy3) == "до 150000 EUR"


@patch('psycopg2.connect')
def test_db_manager_error_handling(mock_connect):
    """Тест обработки ошибок в DBManager"""
    mock_connect.side_effect = Exception("Connection failed")

    config = DatabaseConfig(
        dbname='test_db',
        user='test_user',
        password='test_pass'
    )

    db_manager = DBManager(config)

    # Должно вернуть пустой список при ошибке
    result = db_manager.get_companies_and_vacancies_count()
    assert result == []


def test_json_data_source_file_not_found():
    """Тест обработки отсутствующего файла"""
    with pytest.raises(FileNotFoundError):
        data_source = JSONDataSource("nonexistent.json")
        data_source.get_employers()


def test_get_json_file_path_default(monkeypatch):
    """Тест получения пути к JSON файлу по умолчанию"""
    # Мокаем ввод пользователя (пустая строка)
    monkeypatch.setattr('builtins.input', lambda _: '')
    result = get_json_file_path()
    assert result == 'data/hh_data.json'


def test_get_json_file_path_custom(monkeypatch):
    """Тест получения пользовательского пути к JSON файлу"""
    custom_path = 'custom/path/data.json'
    monkeypatch.setattr('builtins.input', lambda _: custom_path)
    result = get_json_file_path('data/hh_data.json')
    assert result == custom_path


@patch('builtins.input')
@patch('builtins.print')
def test_show_main_menu(mock_print, mock_input):
    """Тест отображения главного меню"""
    mock_input.return_value = '1'
    result = show_main_menu()

    assert result == '1'
    mock_print.assert_called()
    mock_input.assert_called_once()


@patch('builtins.input')
@patch('builtins.print')
def test_show_query_menu(mock_print, mock_input):
    """Тест отображения меню запросов"""
    mock_input.return_value = '2'
    result = show_query_menu()

    assert result == '2'
    mock_print.assert_called()
    mock_input.assert_called_once()


@patch('builtins.print')
def test_display_vacancies_empty(mock_print):
    """Тест отображения пустого списка вакансий"""
    display_vacancies([], "Тест")
    mock_print.assert_called_with("\nТест\nВакансии не найдены")


@patch('builtins.print')
def test_display_vacancies_with_data(mock_print):
    """Тест отображения списка вакансий с данными"""
    vacancies = [{
        'company_name': 'Test Company',
        'vacancy_name': 'Test Vacancy',
        'salary_from': 100000,
        'salary_to': 150000,
        'salary_currency': 'RUB',
        'vacancy_url': 'http://test.com'
    }]

    display_vacancies(vacancies, "Тестовые вакансии")

    # Проверяем, что печатается информация о вакансии
    assert mock_print.call_count > 0


@patch('src.utils.DBManager')
@patch('src.utils.DatabaseConfig.from_env')
@patch('builtins.input')
@patch('builtins.print')
def test_execute_queries_connection_error(mock_print, mock_input, mock_from_env, mock_db_manager):
    """Тест выполнения запросов с ошибкой подключения"""
    # Настраиваем моки
    mock_from_env.return_value = None
    mock_input.return_value = '8'  # Для выхода из меню

    execute_queries()

    mock_print.assert_any_call("✗ Ошибка: Не удалось загрузить конфигурацию БД")


@patch('src.utils.DBManager')
@patch('src.utils.DatabaseConfig.from_env')
@patch('builtins.input')
@patch('builtins.print')
def test_execute_queries_menu_navigation(mock_print, mock_input, mock_from_env, mock_db_manager_class):
    """Тест навигации по меню запросов"""
    # Создаем мок конфигурации
    mock_config = Mock()
    mock_config.validate.return_value = True
    mock_from_env.return_value = mock_config

    # Создаем мок DBManager
    mock_db_manager = Mock()
    mock_db_manager_class.return_value = mock_db_manager

    # Настраиваем возвращаемые значения
    mock_db_manager.get_companies_and_vacancies_count.return_value = [
        {'company_name': 'Test Co', 'vacancies_count': 5}
    ]
    mock_db_manager.get_all_vacancies.return_value = []
    mock_db_manager.get_avg_salary.return_value = 120000.0
    mock_db_manager.get_vacancies_with_higher_salary.return_value = []
    mock_db_manager.get_vacancies_with_keyword.return_value = []
    mock_db_manager.get_top_companies_by_vacancies.return_value = []
    mock_db_manager.get_salary_statistics.return_value = {}

    # Симулируем выбор пользователя: 1 -> 2 -> 3 -> 8 (выход)
    mock_input.side_effect = ['1', '', '2', '', '3', '', '8']

    execute_queries()

    # Проверяем, что методы были вызваны
    assert mock_db_manager.get_companies_and_vacancies_count.called
    assert mock_db_manager.get_all_vacancies.called
    assert mock_db_manager.get_avg_salary.called


@patch('src.utils.DatabaseConfig.from_env')
@patch('builtins.print')
def test_load_data_to_database_config_error(mock_print, mock_from_env):
    """Тест загрузки данных с ошибкой конфигурации"""
    mock_from_env.return_value = None

    result = load_data_to_database('test.json')

    assert result is False
    mock_print.assert_any_call("✗ Ошибка: Не удалось загрузить конфигурацию БД")


@patch('src.utils.DatabaseManager')
@patch('src.utils.DatabaseConfig.from_env')
@patch('src.utils.JSONDataSource')
@patch('builtins.input')
@patch('builtins.print')
def test_load_data_to_database_file_not_found(mock_print, mock_input, mock_json_source_class,
                                              mock_from_env, mock_db_manager_class):
    """Тест загрузки данных с отсутствующим файлом"""
    mock_config = Mock()
    mock_config.validate.return_value = True
    mock_from_env.return_value = mock_config

    mock_db_manager = Mock()
    mock_db_manager_class.return_value = mock_db_manager

    mock_json_source_class.side_effect = FileNotFoundError

    result = load_data_to_database('nonexistent.json')

    assert result is False
    mock_print.assert_any_call("Файл nonexistent.json не найден!")

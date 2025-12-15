import pytest
from unittest.mock import Mock, patch, MagicMock
from src.api_client import HTTPRequestHandler, DataSaver, HeadHunterAPIClient
import requests.exceptions


def test_http_request_handler_init():
    """Тест инициализации HTTPRequestHandler"""
    handler = HTTPRequestHandler()
    assert handler._HTTPRequestHandler__base_url == 'https://api.hh.ru/'
    assert handler._HTTPRequestHandler__headers['User-Agent'] == 'CW3/1.0 (aleksey.s.lozhkin@gmail.com.com)'
    assert handler._HTTPRequestHandler__headers['Accept'] == 'application/json'


@patch('requests.get')
def test_http_request_handler_make_request_success(mock_get):
    """Тест успешного выполнения HTTP запроса"""
    mock_response = Mock()
    mock_response.json.return_value = {'key': 'value'}
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    handler = HTTPRequestHandler()
    result = handler.make_request('test/endpoint', {'param': 'value'})

    assert result == {'key': 'value'}
    mock_get.assert_called_once_with(
        'https://api.hh.ru/test/endpoint',
        params={'param': 'value'},
        headers={'User-Agent': 'CW3/1.0 (aleksey.s.lozhkin@gmail.com.com)', 'Accept': 'application/json'}
    )


@patch('requests.get')
def test_http_request_handler_make_request_error(mock_get):
    """Тест обработки ошибки HTTP запроса"""
    # Создаем mock ответа, который вызывает HTTPError
    mock_response = Mock()

    # Создаем HTTPError с response, который имеет status_code
    http_error = requests.exceptions.HTTPError('HTTP Error')
    # Создаем mock response для исключения
    error_response = Mock()
    error_response.status_code = 404
    http_error.response = error_response

    mock_response.raise_for_status.side_effect = http_error
    mock_get.return_value = mock_response

    handler = HTTPRequestHandler()

    with pytest.raises(requests.exceptions.HTTPError):
        handler.make_request('test/endpoint')


@patch('requests.get')
def test_http_request_handler_make_request_connection_error(mock_get):
    """Тест обработки ошибки соединения"""
    mock_get.side_effect = requests.exceptions.RequestException('Connection error')

    handler = HTTPRequestHandler()

    with pytest.raises(requests.exceptions.RequestException):
        handler.make_request('test/endpoint')


def test_data_saver_init():
    """Тест инициализации DataSaver"""
    saver = DataSaver(default_encoding='utf-8', indent=2)
    assert saver._DataSaver__default_encoding == 'utf-8'
    assert saver._DataSaver__indent == 2


@patch('os.path.dirname')
@patch('os.makedirs')
@patch('builtins.open')
@patch('os.path.exists')
def test_data_saver_save_to_json_success(mock_exists, mock_open, mock_makedirs, mock_dirname):
    """Тест успешного сохранения данных в JSON"""
    # Настраиваем моки
    mock_exists.return_value = True
    mock_dirname.return_value = 'data'

    saver = DataSaver()
    data = {'test': 'data'}

    result = saver.save_to_json(data, 'data/test.json')

    assert result is True
    mock_makedirs.assert_called_once_with('data', exist_ok=True)
    mock_open.assert_called_once_with('data/test.json', 'w', encoding='utf-8')


@patch('os.path.dirname')
@patch('os.makedirs')
@patch('builtins.open')
@patch('os.path.exists')
def test_data_saver_save_to_json_file_not_created(mock_exists, mock_open, mock_makedirs, mock_dirname):
    """Тест когда файл не был создан"""
    mock_exists.return_value = False  # Файл не создался
    mock_dirname.return_value = 'data'

    saver = DataSaver()
    data = {'test': 'data'}

    result = saver.save_to_json(data, 'data/test.json')

    assert result is False


@patch('os.path.dirname')
@patch('os.makedirs')
def test_data_saver_save_to_json_os_error(mock_makedirs, mock_dirname):
    """Тест ошибки файловой системы при сохранении"""
    mock_dirname.return_value = 'data'
    mock_makedirs.side_effect = OSError('Permission denied')

    saver = DataSaver()
    data = {'test': 'data'}

    result = saver.save_to_json(data, 'data/test.json')

    assert result is False


def test_hh_api_client_init():
    """Тест инициализации HeadHunterAPIClient"""
    client = HeadHunterAPIClient()
    assert hasattr(client, '_request_handler')
    assert hasattr(client, '_data_saver')
    assert isinstance(client._request_handler, HTTPRequestHandler)
    assert isinstance(client._data_saver, DataSaver)


@patch.object(HTTPRequestHandler, 'make_request')
def test_get_employer_info_success(mock_make_request):
    """Тест успешного получения информации о компании"""
    mock_data = {'id': '123', 'name': 'Test Company'}
    mock_make_request.return_value = mock_data

    client = HeadHunterAPIClient()
    result = client.get_employer_info('123')

    assert result == mock_data
    mock_make_request.assert_called_once_with('employers/123')


@patch.object(HTTPRequestHandler, 'make_request')
def test_get_employer_info_error(mock_make_request):
    """Тест получения информации о компании с ошибкой"""
    mock_make_request.side_effect = requests.exceptions.HTTPError('API Error')

    client = HeadHunterAPIClient()
    result = client.get_employer_info('123')

    assert result is None


@patch.object(HTTPRequestHandler, 'make_request')
@patch('time.sleep')
def test_get_employer_vacancies(mock_sleep, mock_make_request):
    """Тест получения вакансий компании"""
    mock_data = {
        'items': [{'id': '1', 'name': 'Vacancy 1'}],
        'pages': 1
    }
    mock_make_request.return_value = mock_data

    client = HeadHunterAPIClient()
    result = client.get_employer_vacancies('123', per_page=50)

    assert len(result) == 1
    assert result[0]['id'] == '1'
    mock_make_request.assert_called_once_with(
        'vacancies',
        {'employer_id': '123', 'page': 0, 'per_page': 50}
    )


@patch.object(HeadHunterAPIClient, 'get_employer_info')
@patch.object(HeadHunterAPIClient, 'get_employer_vacancies')
@patch('time.sleep')
def test_get_companies_data(mock_sleep, mock_get_vacancies, mock_get_info):
    """Тест получения данных компаний"""
    # Первая компания с вакансиями
    mock_employer1 = {'id': '123', 'name': 'Test Company'}
    mock_vacancies1 = [{'id': '1', 'name': 'Vacancy 1'}]

    # Вторая компания без вакансий
    mock_employer2 = {'id': '456', 'name': 'Test Company 2'}
    mock_vacancies2 = []

    mock_get_info.side_effect = [mock_employer1, mock_employer2]
    mock_get_vacancies.side_effect = [mock_vacancies1, mock_vacancies2]

    client = HeadHunterAPIClient()
    result = client.get_companies_data(['123', '456'])

    assert len(result) == 2
    assert result[0]['employer'] == mock_employer1
    assert result[0]['vacancies'] == mock_vacancies1
    assert result[0]['vacancies_count'] == 1
    assert result[1]['employer'] == mock_employer2
    assert result[1]['vacancies'] == mock_vacancies2
    assert result[1]['vacancies_count'] == 0


@patch.object(DataSaver, 'save_to_json')
def test_save_companies_data(mock_save):
    """Тест сохранения данных компаний"""
    mock_save.return_value = True

    client = HeadHunterAPIClient()
    companies_data = [
        {
            'employer': {'id': '123', 'name': 'Company A'},
            'vacancies': [{'id': '1', 'name': 'Vacancy 1'}],
            'vacancies_count': 1
        }
    ]

    result = client.save_companies_data(companies_data, 'test.json')

    assert result is True
    mock_save.assert_called_once()
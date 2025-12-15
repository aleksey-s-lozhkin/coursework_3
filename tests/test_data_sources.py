import json
from unittest.mock import Mock, patch, mock_open
from src.data_sources import JSONDataSource



# Тесты для JSONDataSource
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

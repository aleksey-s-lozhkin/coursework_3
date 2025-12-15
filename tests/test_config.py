import pytest
import os
from src.config import DatabaseConfig


def test_database_config_creation():
    """Тест создания конфигурации"""
    config = DatabaseConfig(
        dbname='test_db',
        user='test_user',
        password='test_pass',
        host='localhost',
        port=5432
    )

    assert config.dbname == 'test_db'
    assert config.user == 'test_user'
    assert config.password == 'test_pass'
    assert config.host == 'localhost'
    assert config.port == 5432


def test_database_config_from_env(monkeypatch):
    """Тест создания конфигурации из переменных окружения"""
    monkeypatch.setenv('DB_NAME', 'test_db')
    monkeypatch.setenv('DB_USER', 'test_user')
    monkeypatch.setenv('DB_PASSWORD', 'test_pass')
    monkeypatch.setenv('DB_HOST', 'localhost')
    monkeypatch.setenv('DB_PORT', '5432')

    config = DatabaseConfig.from_env()

    assert config is not None
    assert config.dbname == 'test_db'
    assert config.user == 'test_user'
    assert config.password == 'test_pass'
    assert config.host == 'localhost'
    assert config.port == 5432


def test_database_config_from_env_missing_required_vars(monkeypatch):
    """Тест с отсутствующими обязательными переменными окружения"""
    # Случай 1: отсутствует DB_NAME
    monkeypatch.delenv('DB_NAME', raising=False)
    monkeypatch.setenv('DB_USER', 'test_user')
    monkeypatch.setenv('DB_PASSWORD', 'test_pass')

    config = DatabaseConfig.from_env()
    assert config is None  # Должен вернуть None при отсутствии обязательных полей

    # Случай 2: отсутствует DB_USER
    monkeypatch.setenv('DB_NAME', 'test_db')
    monkeypatch.delenv('DB_USER', raising=False)

    config = DatabaseConfig.from_env()
    assert config is None

    # Случай 3: отсутствует DB_PASSWORD
    monkeypatch.setenv('DB_USER', 'test_user')
    monkeypatch.delenv('DB_PASSWORD', raising=False)

    config = DatabaseConfig.from_env()
    assert config is None


def test_database_config_from_env_empty_required_vars(monkeypatch):
    """Тест с пустыми обязательными переменными окружения"""
    monkeypatch.setenv('DB_NAME', '')
    monkeypatch.setenv('DB_USER', 'test_user')
    monkeypatch.setenv('DB_PASSWORD', 'test_pass')

    config = DatabaseConfig.from_env()
    assert config is None  # Пустая строка считается как отсутствующая


def test_database_config_from_env_invalid_port(monkeypatch):
    """Тест создания конфигурации с некорректным портом"""
    monkeypatch.setenv('DB_NAME', 'test_db')
    monkeypatch.setenv('DB_USER', 'test_user')
    monkeypatch.setenv('DB_PASSWORD', 'test_pass')
    monkeypatch.setenv('DB_HOST', 'localhost')
    monkeypatch.setenv('DB_PORT', 'not_a_number')  # Некорректный порт

    config = DatabaseConfig.from_env()

    # Согласно коду, при некорректном порте используется порт по умолчанию 5432
    assert config is not None
    assert config.port == 5432  # Должен использовать значение по умолчанию


def test_database_config_from_env_empty_port(monkeypatch):
    """Тест с пустым портом"""
    monkeypatch.setenv('DB_NAME', 'test_db')
    monkeypatch.setenv('DB_USER', 'test_user')
    monkeypatch.setenv('DB_PASSWORD', 'test_pass')
    monkeypatch.setenv('DB_HOST', 'localhost')
    monkeypatch.setenv('DB_PORT', '')  # Пустой порт

    config = DatabaseConfig.from_env()

    assert config is not None
    assert config.port == 5432  # Должен использовать значение по умолчанию


def test_database_config_from_env_default_values(monkeypatch):
    """Тест значений по умолчанию при отсутствии необязательных переменных"""
    monkeypatch.setenv('DB_NAME', 'test_db')
    monkeypatch.setenv('DB_USER', 'test_user')
    monkeypatch.setenv('DB_PASSWORD', 'test_pass')

    # Удаляем необязательные переменные
    monkeypatch.delenv('DB_HOST', raising=False)
    monkeypatch.delenv('DB_PORT', raising=False)

    config = DatabaseConfig.from_env()

    assert config is not None
    assert config.host == 'localhost'  # Значение по умолчанию
    assert config.port == 5432  # Значение по умолчанию


def test_database_config_validate_valid():
    """Тест валидации корректной конфигурации"""
    config = DatabaseConfig(
        dbname='test_db',
        user='test_user',
        password='test_pass',
        host='localhost',
        port=5432
    )

    assert config.validate() is True


def test_database_config_validate_invalid_dbname():
    """Тест валидации с пустым именем БД"""
    config = DatabaseConfig(
        dbname='',  # Пустое имя БД
        user='test_user',
        password='test_pass',
        host='localhost',
        port=5432
    )

    assert config.validate() is False


def test_database_config_validate_invalid_port():
    """Тест валидации с некорректным портом"""
    # Порт 0
    config1 = DatabaseConfig(
        dbname='test_db',
        user='test_user',
        password='test_pass',
        host='localhost',
        port=0  # Некорректный порт (должен быть 1-65535)
    )
    assert config1.validate() is False

    # Порт отрицательный
    config2 = DatabaseConfig(
        dbname='test_db',
        user='test_user',
        password='test_pass',
        host='localhost',
        port=-1
    )
    assert config2.validate() is False

    # Порт слишком большой
    config3 = DatabaseConfig(
        dbname='test_db',
        user='test_user',
        password='test_pass',
        host='localhost',
        port=70000  # Больше 65535
    )
    assert config3.validate() is False


def test_database_config_validate_invalid_user():
    """Тест валидации с пустым пользователем"""
    config = DatabaseConfig(
        dbname='test_db',
        user='',  # Пустой пользователь
        password='test_pass',
        host='localhost',
        port=5432
    )

    assert config.validate() is False


def test_database_config_validate_invalid_password():
    """Тест валидации с пустым паролем"""
    config = DatabaseConfig(
        dbname='test_db',
        user='test_user',
        password='',  # Пустой пароль
        host='localhost',
        port=5432
    )

    assert config.validate() is False


def test_database_config_default_values():
    """Тест значений по умолчанию при создании"""
    config = DatabaseConfig(
        dbname='test_db',
        user='test_user',
        password='test_pass'
    )

    assert config.host == 'localhost'  # Значение по умолчанию
    assert config.port == 5432  # Значение по умолчанию


def test_database_config_frozen():
    """Тест что конфигурация immutable (заморожена)"""
    config = DatabaseConfig(
        dbname='test_db',
        user='test_user',
        password='test_pass'
    )

    # Попытка изменить атрибут должна вызвать ошибку, так как dataclass frozen=True
    with pytest.raises(Exception):
        config.dbname = 'new_db'


def test_database_config_dataclass_methods():
    """Тест методов dataclass"""
    config1 = DatabaseConfig(
        dbname='test_db',
        user='test_user',
        password='test_pass',
        host='localhost',
        port=5432
    )

    config2 = DatabaseConfig(
        dbname='test_db',
        user='test_user',
        password='test_pass',
        host='localhost',
        port=5432
    )

    config3 = DatabaseConfig(
        dbname='other_db',
        user='test_user',
        password='test_pass',
        host='localhost',
        port=5432
    )

    # Проверка равенства
    assert config1 == config2
    assert config1 != config3

    # Проверка хэширования (так как frozen=True)
    assert hash(config1) == hash(config2)
    assert hash(config1) != hash(config3)

    # Проверка репрезентации
    assert 'DatabaseConfig' in repr(config1)
    assert 'test_db' in repr(config1)
    assert 'localhost' in repr(config1)
    assert '5432' in str(config1.port)

import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class DatabaseConfig:
    """Конфигурация подключения к базе данных"""

    dbname: str
    user: str
    password: str
    host: str = "localhost"
    port: int = 5432

    @classmethod
    def from_env(cls) -> Optional["DatabaseConfig"]:
        """
        Создает конфигурацию из переменных окружения.
        """
        # Чтение обязательных параметров
        dbname = os.getenv("DB_NAME")
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")

        # Проверяем наличие всех обязательных полей
        if not dbname or not user or not password:
            print("Ошибка: Отсутствуют обязательные параметры БД в .env файле")
            print("Проверьте наличие DB_NAME, DB_USER, DB_PASSWORD")
            return None

        # Чтение опциональных параметров со значениями по умолчанию
        host = os.getenv("DB_HOST", "localhost")
        port_str = os.getenv("DB_PORT", "5432")

        try:
            port = int(port_str)
        except ValueError:
            print(f"Некорректный порт '{port_str}', используется 5432")
            port = 5432

        return cls(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port,
        )

    def validate(self) -> bool:
        """Проверяет валидность конфигурации"""
        errors = []

        if not self.dbname:
            errors.append("Не указано имя базы данных")
        if not self.user:
            errors.append("Не указан пользователь")
        if not self.password:
            errors.append("Не указан пароль")
        if self.port < 1 or self.port > 65535:
            errors.append(f"Некорректный порт: {self.port}")

        if errors:
            print("Ошибки в конфигурации БД:")
            for error in errors:
                print(f"   - {error}")
            return False

        return True

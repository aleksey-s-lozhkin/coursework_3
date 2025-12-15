from src.config import DatabaseConfig
from src.db_manager import DatabaseManager
from src.utils import main_loop


def initialize_database():
    """Инициализация базы данных - создание БД и таблиц"""
    print("\n=== Инициализация базы данных ===")

    # Загрузка конфигурации БД
    config = DatabaseConfig.from_env()

    if not config:
        print("Ошибка: Не удалось загрузить конфигурацию БД")
        return False

    if not config.validate():
        print("Ошибка: Некорректная конфигурация БД")
        return False

    print(f"Конфигурация загружена: {config.host}:{config.port}/{config.dbname}")

    try:
        # Инициализация менеджера БД (включает создание БД и подключение)
        db_manager = DatabaseManager(config)

        # Создание таблиц
        print("\nСоздание таблиц...")
        db_manager.schema_manager.create_tables()

        print("База данных и таблицы успешно созданы")
        db_manager.close()
        return True

    except Exception as e:
        print(f"Ошибка при создании базы данных: {e}")
        print("\nРекомендации по устранению проблемы:")
        print("1. Проверьте, что PostgreSQL установлен и запущен")
        print("2. Убедитесь, что пользователь из .env файла существует")
        print("3. Проверьте правильность пароля")
        print("4. Попробуйте создать базу данных вручную:")
        print(f'   sudo -u postgres psql -c "CREATE DATABASE {config.dbname};"')
        return False


def main():
    """Точка входа в программу"""
    print("=" * 60)
    print("ПРОГРАММА ДЛЯ РАБОТЫ С ВАКАНСИЯМИ HEADHUNTER")
    print("=" * 60)

    # Инициализация базы данных при запуске программы
    if initialize_database():
        main_loop()
    else:
        print("\nНе удалось инициализировать базу данных.")
        print("Программа будет работать с ограниченной функциональностью.")
        continue_anyway = input("Продолжить? (y/n): ").strip().lower()
        if continue_anyway == 'y':
            main_loop()
        else:
            print("Выход из программы.")


if __name__ == "__main__":
    main()

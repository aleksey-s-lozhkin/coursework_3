import os
import sys
from typing import Optional

from src.api_client import HeadHunterAPIClient
from src.config import DatabaseConfig
from src.data_sources import JSONDataSource
from src.db_manager import DatabaseManager


def collect_data_from_hh() -> Optional[str]:
    """Сбор данных с HeadHunter API"""
    print("\n=== Сбор данных с HeadHunter API ===")
    hh_client = HeadHunterAPIClient()

    target_companies = [
        '15478',  # VK
        '3529',  # Сбер
        '1740',  # Яндекс
        '78638',  # Тинькофф
        '4181',  # Газпром нефть
        '3776',  # МТС
        '39305',  # Ozon
        '87021',  # Wildberries
        '2180',  # Ростелеком
        '64174'  # 1С
    ]

    print(f"Сбор данных по {len(target_companies)} компаниям...")
    try:
        companies_data = hh_client.get_companies_data(target_companies)

        # Сохраняем данные
        output_file = 'data/hh_data.json'
        os.makedirs('data', exist_ok=True)
        hh_client.save_companies_data(companies_data, output_file)

        print(f"✓ Данные успешно сохранены в файл: {output_file}")
        print(f"   Найдено: {len(companies_data['employers'])} работодателей")
        print(f"   Найдено: {len(companies_data['vacancies'])} вакансий")

        return output_file
    except Exception as e:
        print(f"✗ Ошибка при сборе данных: {e}")
        return None


def load_data_to_database(json_file: str) -> bool:
    """Загрузка данных из JSON в базу данных"""
    print("\n=== Загрузка данных в базу данных ===")

    # Загрузка конфигурации БД
    print("\n1. Загрузка конфигурации базы данных...")
    config = DatabaseConfig.from_env()

    if not config:
        print("✗ Ошибка: Не удалось загрузить конфигурацию БД")
        return False

    if not config.validate():
        print("✗ Ошибка: Некорректная конфигурация БД")
        return False

    print(f"✓ Конфигурация загружена: {config.host}:{config.port}/{config.dbname}")

    try:
        # Инициализация менеджера БД (включает создание БД и подключение)
        db_manager = DatabaseManager(config)

        # Создание таблиц
        db_manager.schema_manager.create_tables()

        # Загрузка данных из JSON
        print(f"\n2. Загрузка данных из файла: {json_file}")

        try:
            data_source = JSONDataSource(json_file)
        except FileNotFoundError:
            print(f"✗ Файл {json_file} не найден!")
            db_manager.close()
            return False
        except Exception as e:
            print(f"✗ Ошибка при чтении файла {json_file}: {e}")
            db_manager.close()
            return False

        # Проверка наличия данных
        employers = data_source.get_employers()
        vacancies = data_source.get_vacancies()

        if not employers and not vacancies:
            print(f"✗ Файл {json_file} не содержит данных или имеет неверный формат")
            db_manager.close()
            return False

        print(f"✓ Найдено: {len(employers)} работодателей и {len(vacancies)} вакансий")

        # Подтверждение загрузки
        confirm = input(f"\nЗагрузить данные в БД? (y/n): ").strip().lower()
        if confirm != 'y':
            print("✗ Отменено пользователем")
            db_manager.close()
            return False

        # Загрузка данных
        print("\n3. Загрузка данных в таблицы...")
        stats = db_manager.load_data_from_json(data_source)

        # Вывод статистики
        print("\n" + "=" * 50)
        print("СТАТИСТИКА ЗАГРУЗКИ:")
        print("=" * 50)
        print(f"Работодатели: {stats['employers_loaded']}/{stats['employers_total']}")
        print(f"Вакансии:     {stats['vacancies_loaded']}/{stats['vacancies_total']}")

        if stats['employers_loaded'] < stats['employers_total']:
            print(f"\n⚠  Пропущено работодателей: {stats['employers_total'] - stats['employers_loaded']} (дубликаты)")
        if stats['vacancies_loaded'] < stats['vacancies_total']:
            print(
                f"⚠  Пропущено вакансий: {stats['vacancies_total'] - stats['vacancies_loaded']} (дубликаты или ошибки)")

        print(f"\n✓ Данные успешно загружены в базу данных '{config.dbname}'")
        return True

    except Exception as e:
        print(f"\n✗ Критическая ошибка при работе с базой данных: {e}")
        print("\nРекомендации по устранению проблемы:")
        print("1. Проверьте, что PostgreSQL установлен и запущен")
        print("2. Убедитесь, что пользователь из .env файла существует")
        print("3. Проверьте правильность пароля")
        print("4. Попробуйте создать базу данных вручную:")
        print(f'   sudo -u postgres psql -c "CREATE DATABASE {config.dbname};"')
        return False
    finally:
        # Закрытие соединения
        try:
            if 'db_manager' in locals():
                db_manager.close()
        except:
            pass


def get_json_file_path(default_file: str = 'data/hh_data.json') -> str:
    """Получение пути к JSON файлу от пользователя"""
    json_file = input(f"Введите путь к JSON файлу (по умолчанию: {default_file}): ").strip()
    return json_file if json_file else default_file


def show_main_menu() -> str:
    """Отображение главного меню и получение выбора пользователя"""
    print("\n" + "=" * 60)
    print("ГЛАВНОЕ МЕНЮ")
    print("=" * 60)
    print("1. Собрать данные с HeadHunter API")
    print("2. Загрузить данные из JSON в базу данных")
    print("3. Выйти из программы")

    return input("\nВыберите действие (1-3): ").strip()
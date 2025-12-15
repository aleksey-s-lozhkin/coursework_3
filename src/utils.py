import os
from typing import Any, Dict, List, Optional, cast

from src.api_client import HeadHunterAPIClient
from src.config import DatabaseConfig
from src.data_sources import JSONDataSource
from src.db_manager import DatabaseManager
from src.db_queries import DBManager


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
        '64174',  # 1С
    ]

    print(f"Сбор данных по {len(target_companies)} компаниям...")
    try:
        companies_data = hh_client.get_companies_data(target_companies)

        # Преобразуем структуру данных в нужный формат
        formatted_data: Dict[str, Any] = {
            'employers': [],
            'vacancies': [],
            'metadata': {'total_companies': len(target_companies), 'saved_at': '2024-01-01 00:00:00'},
        }

        # Приводим типы для корректной работы mypy
        employers_list = cast(List[Dict[str, Any]], formatted_data['employers'])
        vacancies_list = cast(List[Dict[str, Any]], formatted_data['vacancies'])

        for company_data in companies_data:
            # Добавляем работодателя
            employer = company_data.get('employer')
            if employer:
                employers_list.append(employer)

            # Добавляем вакансии
            vacancies = company_data.get('vacancies', [])
            if vacancies:
                vacancies_list.extend(vacancies)

        # Сохраняем данные
        output_file = 'data/hh_data.json'
        os.makedirs('data', exist_ok=True)
        with open(output_file, 'w', encoding='utf-8') as f:
            import json

            json.dump(formatted_data, f, ensure_ascii=False, indent=2)

        print(f"Данные успешно сохранены в файл: {output_file}")
        print(f"   Найдено: {len(employers_list)} работодателей")
        print(f"   Найдено: {len(vacancies_list)} вакансий")

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
        # Инициализация менеджера БД
        db_manager = DatabaseManager(config)

        # Загрузка данных из JSON
        print(f"\n2. Загрузка данных из файла: {json_file}")

        try:
            data_source = JSONDataSource(json_file)
        except FileNotFoundError:
            print(f"Файл {json_file} не найден!")
            db_manager.close()
            return False
        except Exception as e:
            print(f"Ошибка при чтении файла {json_file}: {e}")
            db_manager.close()
            return False

        # Проверка наличия данных
        employers = data_source.get_employers()
        vacancies = data_source.get_vacancies()

        if not employers and not vacancies:
            print(f"Файл {json_file} не содержит данных или имеет неверный формат")
            db_manager.close()
            return False

        print(f"✓ Найдено: {len(employers)} работодателей и {len(vacancies)} вакансий")

        # Подтверждение загрузки
        confirm = input("\nЗагрузить данные в БД? (y/n): ").strip().lower()
        if confirm != 'y':
            print("Отменено пользователем")
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
            print(f"\nПропущено работодателей: {stats['employers_total'] - stats['employers_loaded']} (дубликаты)")
        if stats['vacancies_loaded'] < stats['vacancies_total']:
            print(
                f"Пропущено вакансий: {stats['vacancies_total'] - stats['vacancies_loaded']} (дубликаты или ошибки)"
            )

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
        except Exception:
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
    print("3. Выполнить запросы к базе данных")
    print("4. Выйти из программы")

    return input("\nВыберите действие (1-4): ").strip()


def show_query_menu() -> str:
    """Отображение меню запросов к базе данных"""
    print("\n" + "=" * 60)
    print("МЕНЮ ЗАПРОСОВ К БАЗЕ ДАННЫХ")
    print("=" * 60)
    print("1. Показать компании и количество вакансий")
    print("2. Показать все вакансии")
    print("3. Показать среднюю зарплату")
    print("4. Показать вакансии с зарплатой выше средней")
    print("5. Поиск вакансий по ключевому слову")
    print("6. Показать топ компаний по количеству вакансий")
    print("7. Показать статистику зарплат")
    print("8. Вернуться в главное меню")

    return input("\nВыберите действие (1-8): ").strip()


def format_salary(vacancy: Dict[str, Any]) -> str:
    """Форматирование зарплаты для отображения"""
    if vacancy['salary_from'] or vacancy['salary_to']:
        if vacancy['salary_from'] and vacancy['salary_to']:
            return f"{vacancy['salary_from']} - {vacancy['salary_to']} {vacancy['salary_currency']}"
        elif vacancy['salary_from']:
            return f"от {vacancy['salary_from']} {vacancy['salary_currency']}"
        elif vacancy['salary_to']:
            return f"до {vacancy['salary_to']} {vacancy['salary_currency']}"
    return "не указана"


def display_vacancies(vacancies: List[Dict[str, Any]], title: str):
    """Отображение списка вакансий"""
    if vacancies:
        print(f"\n{title} ({len(vacancies)} вакансий):")
        print("-" * 70)
        for vacancy in vacancies:
            salary = format_salary(vacancy)
            print(f"Компания: {vacancy['company_name']}")
            print(f"Вакансия: {vacancy['vacancy_name']}")
            print(f"Зарплата: {salary}")
            print(f"Ссылка: {vacancy['vacancy_url']}")
            print("-" * 70)
    else:
        print(f"\n{title}\nВакансии не найдены")


def execute_queries():
    """Выполнение запросов к базе данных"""
    print("\n=== Запросы к базе данных ===")

    # Загрузка конфигурации БД
    config = DatabaseConfig.from_env()

    if not config:
        print("✗ Ошибка: Не удалось загрузить конфигурацию БД")
        return

    if not config.validate():
        print("✗ Ошибка: Некорректная конфигурация БД")
        return

    # Создание менеджера запросов
    try:
        db_manager = DBManager(config)

        while True:
            choice = show_query_menu()

            if choice == '1':
                # Компании и количество вакансий
                print("\n--- Компании и количество вакансий ---")
                companies = db_manager.get_companies_and_vacancies_count()

                if companies:
                    print(f"\nНайдено {len(companies)} компаний:")
                    print("-" * 50)
                    for company in companies:
                        print(f"{company['company_name']}: {company['vacancies_count']} вакансий")
                else:
                    print("Данные о компаниях не найдены")

            elif choice == '2':
                # Все вакансии
                vacancies = db_manager.get_all_vacancies()
                display_vacancies(vacancies, "--- Все вакансии ---")

            elif choice == '3':
                # Средняя зарплата
                print("\n--- Средняя зарплата ---")
                avg_salary = db_manager.get_avg_salary()

                if avg_salary:
                    print(f"Средняя зарплата по всем вакансиям: {avg_salary:.2f} руб.")
                else:
                    print("Не удалось рассчитать среднюю зарплату")

            elif choice == '4':
                # Вакансии с зарплатой выше средней
                vacancies = db_manager.get_vacancies_with_higher_salary()
                display_vacancies(vacancies, "--- Вакансии с зарплатой выше средней ---")

            elif choice == '5':
                # Поиск вакансий по ключевому слову
                print("\n--- Поиск вакансий по ключевому слову ---")
                keyword = input("Введите ключевое слово для поиска: ").strip()

                if keyword:
                    vacancies = db_manager.get_vacancies_with_keyword(keyword)
                    display_vacancies(vacancies, f"--- Результаты поиска по '{keyword}' ---")
                else:
                    print("Ключевое слово не указано")

            elif choice == '6':
                # Топ компаний
                print("\n--- Топ компаний по количеству вакансий ---")
                limit_input = input("Сколько компаний показать (по умолчанию 10): ").strip()
                limit = int(limit_input) if limit_input.isdigit() else 10

                companies = db_manager.get_top_companies_by_vacancies(limit)

                if companies:
                    print(f"\nТоп-{limit} компаний по количеству вакансий:")
                    print("-" * 60)
                    for i, company in enumerate(companies, 1):
                        print(f"{i}. {company['company_name']}: {company['vacancies_count']} вакансий")
                        if company.get('company_url'):
                            print(f"   Ссылка на компанию: {company['company_url']}")
                        print("-" * 60)
                else:
                    print("Данные о компаниях не найдены")

            elif choice == '7':
                # Статистика зарплат
                print("\n--- Статистика зарплат ---")
                stats = db_manager.get_salary_statistics()

                if stats:
                    print("\nСтатистика по зарплатам:")
                    print("-" * 40)
                    print(f"Всего вакансий: {stats.get('total_vacancies', 0)}")
                    print(f"Вакансий с указанием зарплаты: {stats.get('vacancies_with_salary', 0)}")

                    if stats.get('avg_salary'):
                        print(f"Средняя зарплата: {stats['avg_salary']:.2f} руб.")
                        print(f"Минимальная зарплата: {stats['min_salary']:.2f} руб.")
                        print(f"Максимальная зарплата: {stats['max_salary']:.2f} руб.")
                    else:
                        print("Нет данных о зарплатах")
                else:
                    print("Не удалось получить статистику зарплат")

            elif choice == '8':
                print("\nВозвращаемся в главное меню...")
                break

            else:
                print("\n✗ Неверный выбор. Пожалуйста, выберите от 1 до 8.")

            # Пауза перед следующим выбором
            if choice != '8':
                input("\nНажмите Enter для продолжения...")

    except Exception as e:
        print(f"✗ Ошибка при подключении к базе данных: {e}")


def main_loop():
    """Главный цикл программы"""
    print("\nБаза данных инициализирована. Можете начинать работу.")

    while True:
        choice = show_main_menu()

        if choice == '1':
            # Сбор данных с HH
            json_file = collect_data_from_hh()
            if json_file:
                # Предложить сразу загрузить в БД
                load_now = input(f"\nЗагрузить данные из {json_file} в БД сейчас? (y/n): ").strip().lower()
                if load_now == 'y':
                    load_data_to_database(json_file)

        elif choice == '2':
            # Загрузка данных из JSON в БД
            json_file = get_json_file_path()
            load_data_to_database(json_file)

        elif choice == '3':
            # Выполнение запросов к БД
            execute_queries()

        elif choice == '4':
            print("\nВыход из программы. До свидания!")
            break

        else:
            print("\n✗ Неверный выбор. Пожалуйста, выберите 1, 2, 3 или 4.")

        # Пауза перед следующим циклом меню
        if choice != '4':
            input("\nНажмите Enter для продолжения...")

from src.api_client import HeadHunterAPIClient
from src.config import DatabaseConfig
from src.data_sources import JSONDataSource
from src.db_manager import DatabaseManager
import sys

def main():
    # # Создаем клиент
    # hh_client = HeadHunterAPIClient()
    #
    # # Список компаний для анализа
    # target_companies = ['15478', '3529', '1740', '78638', '4181', '3776', '39305', '87021', '2180', '64174']
    #
    # # Базовый сбор данных
    # print("=== Базовый сбор данных ===")
    # companies_data = hh_client.get_companies_data(target_companies)
    # hh_client.save_companies_data(companies_data, 'data/basic_data.json')

    # 1. Загрузка конфигурации БД
    print("Загрузка конфигурации базы данных...")
    config = DatabaseConfig.from_env()

    if not config:
        print("Ошибка: Не удалось загрузить конфигурацию БД")
        sys.exit(1)

    if not config.validate():
        print("Ошибка: Некорректная конфигурация БД")
        sys.exit(1)

    # 2. Инициализация менеджера БД
    print("Подключение к базе данных...")
    try:
        db_manager = DatabaseManager(config)
    except Exception as e:
        print(f"Ошибка подключения к БД: {e}")
        sys.exit(1)

    # 3. Создание таблиц
    print("Создание таблиц...")
    db_manager.schema_manager.create_tables()

    # 4. Загрузка данных из JSON
    print("\nЗагрузка данных из JSON файла...")
    json_file = input("Введите путь к JSON файлу (или нажмите Enter для использования 'data.json'): ").strip()

    if not json_file:
        json_file = "data.json"

    try:
        data_source = JSONDataSource(json_file)

        # Проверка наличия данных
        employers = data_source.get_employers()
        vacancies = data_source.get_vacancies()

        if not employers and not vacancies:
            print(f"Файл {json_file} не содержит данных или имеет неверный формат")
            db_manager.close()
            sys.exit(1)

        print(f"Найдено: {len(employers)} работодателей и {len(vacancies)} вакансий")

        # Подтверждение
        confirm = input(f"\nЗагрузить данные в БД? (y/n): ").strip().lower()
        if confirm != 'y':
            print("Отменено пользователем")
            db_manager.close()
            return

        # Загрузка данных
        stats = db_manager.load_data_from_json(data_source)

        # Вывод статистики
        print("\n" + "=" * 50)
        print("СТАТИСТИКА ЗАГРУЗКИ:")
        print("=" * 50)
        print(f"Работодатели: {stats['employers_loaded']}/{stats['employers_total']}")
        print(f"Вакансии:     {stats['vacancies_loaded']}/{stats['vacancies_total']}")

        if stats['employers_loaded'] < stats['employers_total']:
            print(
                f"\n⚠  Пропущено работодателей: {stats['employers_total'] - stats['employers_loaded']} (возможно, дубликаты)")
        if stats['vacancies_loaded'] < stats['vacancies_total']:
            print(
                f"⚠  Пропущено вакансий: {stats['vacancies_total'] - stats['vacancies_loaded']} (возможно, дубликаты или ошибки)")

    except FileNotFoundError:
        print(f"Файл {json_file} не найден!")
    except Exception as e:
        print(f"Ошибка при загрузке данных: {e}")
    finally:
        # 5. Закрытие соединения
        db_manager.close()
        print("\nСоединение с БД закрыто")


if __name__ == "__main__":
    main()

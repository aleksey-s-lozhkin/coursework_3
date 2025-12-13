from src.utils import (
    collect_data_from_hh,
    load_data_to_database,
    get_json_file_path,
    show_main_menu
)


def main():
    """Главная функция программы"""
    print("=" * 60)
    print("ПРОГРАММА ДЛЯ РАБОТЫ С ВАКАНСИЯМИ HEADHUNTER")
    print("=" * 60)

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
            print("\nВыход из программы. До свидания2!")
            break

        else:
            print("\n✗ Неверный выбор. Пожалуйста, выберите 1, 2 или 3.")

        # Пауза перед следующим циклом меню
        if choice != '3':
            input("\nНажмите Enter для продолжения...")


if __name__ == "__main__":
    main()
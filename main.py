from src.api_client import HeadHunterAPIClient

def main():
    # Создаем клиент
    hh_client = HeadHunterAPIClient()

    # Список компаний для анализа
    target_companies = ['15478', '3529', '1740', '78638', '4181', '3776', '39305', '87021', '2180', '64174']

    # Базовый сбор данных
    print("=== Базовый сбор данных ===")
    companies_data = hh_client.get_companies_data(target_companies)
    hh_client.save_companies_data(companies_data, 'data/basic_data.json')


if __name__ == "__main__":
    main()

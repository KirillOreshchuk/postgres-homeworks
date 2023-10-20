import csv
import psycopg2

"""Скрипт для заполнения данными таблиц в БД Postgres."""


def csv_reader(file_csv):
    """
    Считывет данные из csv файла и добавляет их в список
    """
    result = []
    with open(file_csv, encoding='UTF-8') as file:
        reader = csv.DictReader(file)
        for item in reader:
            result.append(item)

    return result


def append_data_to_tables():
    """
    Добавляет данные из csv файлов в таблицы
    """
    conn = psycopg2.connect(host='localhost', database='north', user='postgres', password='1432')
    try:
        with conn:
            with conn.cursor() as cur:
                for row in csv_reader('north_data/customers_data.csv'):
                    cur.execute('INSERT INTO customers_data Values(%s, %s, %s)',
                                (row['customer_id'], row['company_name'], row['contact_name']))

            with conn.cursor() as cur:
                for row in csv_reader('north_data/employees_data.csv'):
                    cur.execute("INSERT INTO employees_data Values(%s, %s, %s, %s, %s, %s)",
                                (row['employee_id'], row['first_name'], row['last_name'],
                                 row['title'], row['birth_date'], row['notes']))

            with conn.cursor() as cur:
                for row in csv_reader('north_data/orders_data.csv'):
                    cur.execute('INSERT INTO orders_data Values(%s, %s, %s, %s, %s)',
                                (row['order_id'], row['customer_id'], row['employee_id'],
                                 row['order_date'], row['ship_city']))

    finally:
        conn.close()


append_data_to_tables()

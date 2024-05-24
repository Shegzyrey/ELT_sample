import random
import logging
from datetime import datetime

import psycopg2
from faker import Faker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



def generate_fake_data(fake, table_name):
    if table_name == 'oltp.customer':
        return {
            'store_id': random.randint(1, 100),
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'address_id': random.randint(1, 100),
            'activebool': random.choice([True, False]),
            'create_date': fake.date(),
            'last_update': fake.date_time(),
            'active': random.randint(1, 100)
        }
    elif table_name == 'oltp.actor':
        return {
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'last_update': fake.date_time()
        }
    elif table_name == 'oltp.category':
        return {
            'name': fake.word(),
            'last_update': fake.date_time()
        }
    elif table_name == 'oltp.film':
        return {
            'title': fake.catch_phrase(),
            'description': fake.text(),
            'release_year': fake.year(),
            'language_id': random.randint(1, 10),
            'original_language_id': random.randint(1, 10),
            'rental_duration': random.randint(1, 10),
            'rental_rate': round(random.uniform(1, 10), 2),
            'length': random.randint(60, 180),
            'replacement_cost': round(random.uniform(10, 50), 2),
        }
    elif table_name == 'oltp.film_actor':
        return {
            'actor_id': random.randint(1, 100),
            'film_id': random.randint(1, 100),
            'last_update': fake.date_time()
        }
    elif table_name == 'oltp.film_category':
        return {
            'film_id': random.randint(1, 100),
            'category_id': random.randint(1, 100),
            'last_update': fake.date_time()
        }
    elif table_name == 'oltp.address':
        return {
            'address': fake.street_address(),
            'address2': fake.secondary_address(),
            'district': fake.city_suffix(),
            'city_id': random.randint(1, 100),
            'postal_code': fake.postalcode(),
            'phone': fake.phone_number(),
            'last_update': fake.date_time()
        }
    elif table_name == 'oltp.city':
        return {
            'city': fake.city(),
            'country_id': random.randint(1, 100),
            'last_update': fake.date_time()
        }
    elif table_name == 'oltp.country':
        return {
            'country': fake.country(),
            'last_update': fake.date_time()
        }
    elif table_name == 'oltp.inventory':
        return {
            'film_id': random.randint(1, 100),
            'store_id': random.randint(1, 10),
            'last_update': fake.date_time()
        }
    elif table_name == 'oltp.language':
        return {
            'name': fake.word(),
            'last_update': fake.date_time()
        }
    elif table_name == 'oltp.payment':
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 6, 30)
        return {
            'customer_id': random.randint(1, 100),
            'staff_id': random.randint(1, 100),
            'rental_id': random.randint(1, 100),
            'amount': round(random.uniform(1, 100), 2),
            'payment_date': fake.date_time_between(start_date=start_date, end_date=end_date)
        }
    elif table_name == 'oltp.payment_p2023_01':
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 1, 31)
        return {
            'customer_id': random.randint(1, 100),
            'staff_id': random.randint(1, 100),
            'rental_id': random.randint(1, 100),
            'amount': round(random.uniform(1, 100), 2),
            'payment_date': fake.date_time_between(start_date=start_date, end_date=end_date)
        }
    elif table_name == 'oltp.payment_p2023_02':
        start_date = datetime(2023, 2, 1)
        end_date = datetime(2023, 2, 28)
        return {
            'customer_id': random.randint(1, 100),
            'staff_id': random.randint(1, 100),
            'rental_id': random.randint(1, 100),
            'amount': round(random.uniform(1, 100), 2),
            'payment_date': fake.date_time_between(start_date=start_date, end_date=end_date)
        }
    elif table_name == 'oltp.payment_p2023_03':
        start_date = datetime(2023, 3, 1)
        end_date = datetime(2023, 3, 31)
        return {
            'customer_id': random.randint(1, 100),
            'staff_id': random.randint(1, 100),
            'rental_id': random.randint(1, 100),
            'amount': round(random.uniform(1, 100), 2),
            'payment_date': fake.date_time_between(start_date=start_date, end_date=end_date)
        }
    elif table_name == 'oltp.payment_p2023_04':
        start_date = datetime(2023, 4, 1)
        end_date = datetime(2023, 4, 30)
        return {
            'customer_id': random.randint(1, 100),
            'staff_id': random.randint(1, 100),
            'rental_id': random.randint(1, 100),
            'amount': round(random.uniform(1, 100), 2),
            'payment_date': fake.date_time_between(start_date=start_date, end_date=end_date)
        }
    elif table_name == 'oltp.payment_p2023_05':
        start_date = datetime(2023, 5, 1)
        end_date = datetime(2023, 5, 31)
        return {
            'customer_id': random.randint(1, 100),
            'staff_id': random.randint(1, 100),
            'rental_id': random.randint(1, 100),
            'amount': round(random.uniform(1, 100), 2),
            'payment_date': fake.date_time_between(start_date=start_date, end_date=end_date)
        }
    elif table_name == 'oltp.payment_p2023_06':
        start_date = datetime(2023, 6, 1)
        end_date = datetime(2023, 6, 30)
        return {
            'customer_id': random.randint(1, 100),
            'staff_id': random.randint(1, 100),
            'rental_id': random.randint(1, 100),
            'amount': round(random.uniform(1, 100), 2),
            'payment_date': fake.date_time_between(start_date=start_date, end_date=end_date)
        }
    elif table_name == 'oltp.rental':
        return {
            'rental_date': fake.date_time(),
            'inventory_id': random.randint(1, 100),
            'customer_id': random.randint(1, 100),
            'return_date': fake.date_time(),
            'staff_id': random.randint(1, 100),
            'last_update': fake.date_time()
        }
    elif table_name == 'oltp.staff':
        return {
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'address_id': random.randint(1, 100),
            'email': fake.email(),
            'store_id': random.randint(1, 10),
            'active': random.choice([True, False]),
            'username': fake.user_name(),
            'password': fake.password(),
            'last_update': fake.date_time()
        }
    elif table_name == 'oltp.store':
        return {
            'manager_staff_id': random.randint(1, 100),
            'address_id': random.randint(1, 100),
            'last_update': fake.date_time()
        }



def insert_data(conn, cur, table_name, data, unique_columns):
    columns = ', '.join(data.keys())
    values = ', '.join(['%({})s'.format(k) for k in data.keys()])
    query = f"INSERT INTO {table_name} ({columns}) VALUES ({values});"
      #ON CONFLICT ({unique_columns[table_name]}) DO NOTHING;"
    try:
        cur.execute(query, data)
        conn.commit()
        logger.info(f"Inserted data into {table_name}: {data}")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error inserting data into {table_name}: {e}")
        raise


def connect_to_database():
    try:
        conn = psycopg2.connect(
            dbname='oltp_db',
            user='postgres',
            password='Root#123',
            host='oltp-db',
            port=5432
        )
        cur = conn.cursor()
        return conn, cur
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        raise


def close_database_connection(conn, cur):
    cur.close()
    conn.close()


def main():
    try:
        conn, cur = connect_to_database()

        if conn is None or cur is None:
            logger.error("Cannot proceed without a valid database connection.")
            return

        fake = Faker()

        tables = [
            'oltp.country',
            'oltp.city',
            'oltp.address',
            'oltp.language',
            'oltp.customer',
            'oltp.staff',
            'oltp.store',
            'oltp.inventory',
            'oltp.film',
            'oltp.actor',
            'oltp.film_actor',
            'oltp.category',
            'oltp.film_category',
            'oltp.rental',
            'oltp.payment',
            'oltp.payment_p2023_01',
            'oltp.payment_p2023_02',
            'oltp.payment_p2023_03',
            'oltp.payment_p2023_04',
            'oltp.payment_p2023_05',
            'oltp.payment_p2023_06',  
        ]

        unique_columns = {
            'oltp.country': 'country_id',
            'oltp.city': 'city_id',
            'oltp.address': 'address_id',
            'oltp.language': 'language_id',
            'oltp.customer': 'customer_id',
            'oltp.staff': 'staff_id',
            'oltp.store': 'store_id',
            'oltp.inventory': 'inventory_id',
            'oltp.film': 'film_id',
            'oltp.actor': 'actor_id',
            'oltp.film_actor': 'actor_id',
            'oltp.category': 'category_id',
            'oltp.film_category': 'film_id',
            'oltp.rental': 'rental_id',
            'oltp.payment': 'payment_id',
            'oltp.payment_p2023_01': 'payment_id',
            'oltp.payment_p2023_02': 'payment_id',
            'oltp.payment_p2023_03': 'payment_id',
            'oltp.payment_p2023_04': 'payment_id',
            'oltp.payment_p2023_05': 'payment_id',
            'oltp.payment_p2023_06': 'payment_id',
        }

        for table in tables:
            for _ in range(1000):
                data = generate_fake_data(fake, table)
                insert_data(conn, cur, table, data, unique_columns)

    except Exception as e:
        print(f"Error: {e}")

    finally:
        close_database_connection(conn, cur)


if __name__ == "__main__":
    main()

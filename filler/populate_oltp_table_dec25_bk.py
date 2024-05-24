import random
from datetime import datetime, timedelta

import psycopg2
from faker import Faker


# Function to generate fake data for each table
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
        return {
            'customer_id': random.randint(1, 100),
            'staff_id': random.randint(1, 100),
            'rental_id': random.randint(1, 100),
            'amount': round(random.uniform(1, 100), 2),
            'payment_date': fake.date_time()
        }
    elif table_name == 'oltp.payment_p2023_01':
        return {
            'customer_id': random.randint(1, 100),
            'staff_id': random.randint(1, 100),
            'rental_id': random.randint(1, 100),
            'amount': round(random.uniform(1, 100), 2),
            'payment_date': fake.date_time()
        }
    elif table_name == 'oltp.payment_p2023_02':
        return {
            'customer_id': random.randint(1, 100),
            'staff_id': random.randint(1, 100),
            'rental_id': random.randint(1, 100),
            'amount': round(random.uniform(1, 100), 2),
            'payment_date': fake.date_time()
        }
    elif table_name == 'oltp.payment_p2023_03':
        return {
            'customer_id': random.randint(1, 100),
            'staff_id': random.randint(1, 100),
            'rental_id': random.randint(1, 100),
            'amount': round(random.uniform(1, 100), 2),
            'payment_date': fake.date_time()
        }
    elif table_name == 'oltp.payment_p2023_04':
        return {
            'customer_id': random.randint(1, 100),
            'staff_id': random.randint(1, 100),
            'rental_id': random.randint(1, 100),
            'amount': round(random.uniform(1, 100), 2),
            'payment_date': fake.date_time()
        }
    elif table_name == 'oltp.payment_p2023_05':
        return {
            'customer_id': random.randint(1, 100),
            'staff_id': random.randint(1, 100),
            'rental_id': random.randint(1, 100),
            'amount': round(random.uniform(1, 100), 2),
            'payment_date': fake.date_time()
        }
    elif table_name == 'oltp.payment_p2023_06':
        return {
            'customer_id': random.randint(1, 100),
            'staff_id': random.randint(1, 100),
            'rental_id': random.randint(1, 100),
            'amount': round(random.uniform(1, 100), 2),
            'payment_date': fake.date_time()
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


# Function to insert data into a table
def insert_data(conn, cur, table_name, data, unique_columns):
    columns = ', '.join(data.keys())
    values = ', '.join(['%({})s'.format(k) for k in data.keys()])
    query = f"INSERT INTO {table_name} ({columns}) VALUES ({values}) ON CONFLICT ({unique_columns}) DO NOTHING;"
    cur.execute(query, data)
    conn.commit()

try:
    # Connect to the databases
    conn = psycopg2.connect(
        dbname='oltp_db',
        user='postgres',
        password='Root#123',
        host='oltp-db',
        port=5432
    )

    cur = conn.cursor()

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
    'oltp.country': ['country_id'],
    'oltp.city': ['city_id'],
    'oltp.address': ['address_id'],
    'oltp.language': ['language_id'],
    'oltp.customer': ['customer_id'],
    'oltp.staff': ['staff_id'],
    'oltp.store': ['store_id'],
    'oltp.inventory': ['inventory_id'],
    'oltp.film': ['film_id'],
    'oltp.actor': ['actor_id'],
    'oltp.film_actor': ['actor_id', 'film_id'],
    'oltp.category': ['category_id'],
    'oltp.film_category': ['film_id', 'category_id'],
    'oltp.rental': ['rental_id'],
    'oltp.payment': ['payment_id'],
    'oltp.payment_p2023_01': ['payment_id'],
    'oltp.payment_p2023_02': ['payment_id'],
    'oltp.payment_p2023_03': ['payment_id'],
    'oltp.payment_p2023_04': ['payment_id'],
    'oltp.payment_p2023_05': ['payment_id'],
    'oltp.payment_p2023_06': ['payment_id'],
    }

    for table in tables:
        for _ in range(100):
            data = generate_fake_data(fake, table)
            insert_data(conn, cur, table, data, unique_columns)

except Exception as e:
    print(f"Error: {e}")


finally:
    cur.close()
    conn.close()
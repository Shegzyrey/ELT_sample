from faker import Faker
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import random

fake = Faker()

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname='oltp_db',
    user='postgres',
    password='Root#123',
    host='oltp-db',
    port=5432
)

# Set isolation level to AUTOCOMMIT to create the database
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

# Create the oltp database if it doesn't exist
with conn.cursor() as cursor:
    cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'oltp_db'")
    exists = cursor.fetchone()

    if not exists:
        cursor.execute("CREATE DATABASE oltp_db")

# Connect to the oltp database
conn = psycopg2.connect(
    dbname='oltp_db',
    user='postgres',
    password='Root#123',
    host='oltp-db',
    port=5432
)

# Function to execute SQL queries
def execute_query(query, values=None):
    with conn.cursor() as cursor:
        cursor.execute(query, values)
        conn.commit()

# Function to generate fake data and populate tables
def fill_data(table_name, count):
    for _ in range(count):
        if table_name == 'address':
            columns = ('address', 'address2', 'district', 'city_id', 'postal_code', 'phone', 'last_update')
            values = (
                fake.street_address(),      # address
                fake.secondary_address(),   # address2
                fake.city_suffix(),         # district
                fake.random_int(1, 100),    # city_id
                fake.postcode(),            # postal_code
                fake.phone_number(),        # phone
                fake.date_time_this_decade()  # last_update
            )
        elif table_name == 'actor':
            columns = ('first_name', 'last_name', 'last_update')
            values = (
                fake.first_name(),
                fake.last_name(),
                fake.date_time_this_decade()
            )
        elif table_name == 'category':
            columns = ('name', 'last_update')
            values = (
                fake.word(),                # name
                fake.date_time_this_decade()  # last_update
            )
        elif table_name == 'film':
            columns = ('title', 'description', 'release_year', 'language_id', 'original_language_id', 'rental_duration', 'rental_rate', 'length', 'replacement_cost')
            values = (
                fake.sentence(),            # title
                fake.text(),                # description
                fake.year(),                # release_year
                fake.random_int(1, 100),    # language_id
                fake.random_int(1, 100),    # original_language_id
                fake.random_int(1, 10),     # rental_duration
                fake.random_number(2),      # rental_rate
                fake.random_int(1, 200),    # length
                fake.random_number(2)       # replacement_cost
            )
        elif table_name == 'film_actor':
            columns = ('actor_id', 'film_id', 'last_update')
            values = (
                fake.random_int(1, 100),    # actor_id
                fake.random_int(1, 100),    # film_id
                fake.date_time_this_decade()  # last_update
            )
        elif table_name == 'film_category':
            columns = ('film_id', 'category_id', 'last_update')
            values = (
                fake.random_int(1, 100),    # film_id
                fake.random_int(1, 100),    # category_id
                fake.date_time_this_decade()  # last_update
            )
        elif table_name == 'customer':
            columns = ('store_id', 'first_name', 'last_name', 'email', 'address_id', 'activebool', 'create_date', 'last_update', 'active')
            values = (
                fake.random_int(1, 100),  # store_id as integer
                fake.first_name(),
                fake.last_name(),
                fake.email(),
                fake.random_int(1, 100),
                fake.boolean(),
                fake.date(),
                fake.date_time_this_decade(),
                fake.random_int(1, 100)
            )

        elif table_name == 'city':
            columns = ('city', 'country_id', 'last_update')
            values = (
                fake.city(),                # city
                fake.random_int(1, 100),    # country_id
                fake.date_time_this_decade()  # last_update
            )
        elif table_name == 'country':
            columns = ('country', 'last_update')
            values = (
                fake.country(),             # country
                fake.date_time_this_decade()  # last_update
            )
        elif table_name == 'inventory':
            columns = ('film_id', 'store_id', 'last_update')
            values = (
                fake.random_int(1, 100),    # film_id
                fake.random_int(1, 100),    # store_id
                fake.date_time_this_decade()  # last_update
            )
        elif table_name == 'payment':
            columns = ('customer_id', 'staff_id', 'rental_id', 'amount', 'payment_date')
            values = (
                fake.random_int(1, 100),    # customer_id
                fake.random_int(1, 100),    # staff_id
                fake.random_int(1, 100),    # rental_id
                fake.random_number(2),      # amount
                fake.date_time_this_decade()  # payment_date
            )
        elif table_name == 'language':
            columns = ('name', 'last_update')
            values = (
                fake.word(),                # name
                fake.date_time_this_decade()  # last_update
            )
        elif table_name == 'rental':
            columns = ('rental_date', 'inventory_id', 'customer_id', 'return_date', 'staff_id', 'last_update')
            values = (
                fake.date_time_this_decade(),  # rental_date
                fake.random_int(1, 100),    # inventory_id
                fake.random_int(1, 100),    # customer_id
                fake.date_time_this_decade(),  # return_date
                fake.random_int(1, 100),    # staff_id
                fake.date_time_this_decade()  # last_update
            )

        elif table_name == 'staff':
            columns = ('first_name', 'last_name', 'address_id', 'email', 'store_id', 'active', 'username', 'password', 'last_update')
            values = (
                fake.first_name(),          # first_name
                fake.last_name(),           # last_name
                fake.random_int(1, 100),    # address_id
                fake.email(),               # email
                fake.random_int(1, 100),    # store_id
                fake.boolean(),             # active
                fake.user_name(),           # username
                fake.password(),            # password
                fake.date_time_this_decade()  # last_update
            )
        elif table_name == 'store':
            columns = ('manager_staff_id', 'address_id', 'last_update')
            values = (
                fake.random_int(1, 100),    # manager_staff_id
                fake.random_int(1, 100),    # address_id
                fake.date_time_this_decade()  # last_update
            )
        else:
            print(f'kindly provide the {table_name}')
            return

        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s' for _ in values])}) RETURNING *;"        
        execute_query(query, values)


tables_to_fill = ['actor', 'country','city','address','customer', 'category', 'film', 'film_actor', 'film_category',
                'inventory', 'language', 'payment',
                  'rental', 'staff', 'store']

# Fill each table with a random number of data (between 50 and 150 rows)
for table in tables_to_fill:
    fill_data(table, random.randint(50, 150))

# Close the connection
conn.close()

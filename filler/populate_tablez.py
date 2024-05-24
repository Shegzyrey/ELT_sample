from faker import Faker
import psycopg2
from psycopg2 import sql

fake = Faker()

conn = psycopg2.connect(
    dbname='oltp_postgres',
    user='postgres',
    password='Root#123',
    host='oltp-db',#'172.18.0.2',#'oltp-db',
    port=5432
)

cursor = conn.cursor()

def execute_query(query, values=None):
    cursor.execute(query, values) if values else cursor.execute(query)
    conn.commit()

def create_table():
    query = '''
    CREATE TABLE IF NOT EXISTS customers (
        customer_id SERIAL PRIMARY KEY,
        first_name VARCHAR(80) NOT NULL,
        last_name VARCHAR(80) NOT NULL,
        email VARCHAR(120) NOT NULL,
        create_date DATE NOT NULL,
        last_update TIMESTAMP NOT NULL
    );
    '''
    execute_query(query)

def fill_data(count):
    create_table()
    i = 0
    for _ in range(count):
        query = f'''
        INSERT INTO customers (customer_id,first_name, last_name, email, create_date, last_update)
        VALUES ({i},%s, %s, %s, %s, %s)
        RETURNING customer_id;
        '''
        values = (
            fake.unique.first_name(),
            fake.unique.last_name(),
            fake.email(),
            fake.date(),
            fake.date_time_this_decade()
        )
        cursor.execute(query, values)
        customer_id = cursor.fetchone()[0]
        print(f"Customer with ID {customer_id} added successfully!")
        i += 1

    print(f"{count} data record(s) added successfully!")




if __name__ == '__main__':
    fill_data(10)  # Change the count as needed

from faker import Faker
import psycopg2
import random

fake = Faker()

conn = psycopg2.connect(
    dbname='oltp_postgres',
    user='postgres',
    password='Root#123',
    host='oltp-db',
    port=5432
)

cursor = conn.cursor()

def execute_query(query, values=None):
    cursor.execute(query, values) if values else cursor.execute(query)
    conn.commit()

def create_table():
    query = '''
    CREATE TABLE IF NOT EXISTS customer (
        customer_id SERIAL PRIMARY KEY,
        first_name VARCHAR(80) NOT NULL,
        last_name VARCHAR(80) NOT NULL,
        email VARCHAR(120) NOT NULL,
        create_date DATE NOT NULL,
        last_update TIMESTAMP NOT NULL
    );
    '''
    execute_query(query)

def remove_data():
    query = '''
    TRUNCATE TABLE customer;
    '''
    cursor.execute(query)
    conn.commit()
def fill_customer_data(count):
    create_table()
    #remove_data()
    
    for _ in range(count):
        query = '''
        INSERT INTO customer (first_name, last_name, email, create_date, last_update)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING customer_id;
        '''
        values = (
            fake.unique.first_name(),
            fake.unique.last_name(),
            fake.email(),
            fake.date_of_birth(minimum_age=18, maximum_age=90),
            fake.date_time_this_decade()
        )
        cursor.execute(query, values)
        customer_id = cursor.fetchone()[0]
        print(f"Customer with ID {customer_id} added successfully!")
    conn.commit()
    print(f"{count} data record(s) added successfully!")


# Function to fill the 'staff' table with fake data
def fill_staff_data(count):
    for _ in range(count):
        query = '''
        INSERT INTO staff (first_name, last_name, email, last_update)
        VALUES (%s, %s, %s, %s)
        RETURNING staff_id;
        '''
        values = (
            fake.first_name(),
            fake.last_name(),
            fake.email(),
            fake.date_time_this_decade()
        )
        cursor.execute(query, values)
        staff_id = cursor.fetchone()[0]
        print(f"Staff with ID {staff_id} added successfully!")

    conn.commit()
    print(f"{count} staff record(s) added successfully!")

# Function to fill the 'film' table with fake data
def fill_film_data(count):
    for _ in range(count):
        query = '''
        INSERT INTO film (title, description, release_year, original_language_id, rental_duration, rental_rate, length, replacement_cost, last_update)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING film_id;
        '''
        values = (
            fake.catch_phrase(),
            fake.text(),
            fake.date_this_decade(),
            random.randint(1, 5),
            random.randint(1, 10),
            round(random.uniform(1, 9.99), 2),
            random.randint(60, 180),
            round(random.uniform(9.99, 29.99), 2),
            fake.date_time_this_decade()
        )
        cursor.execute(query, values)
        film_id = cursor.fetchone()[0]
        print(f"Film with ID {film_id} added successfully!")

    conn.commit()
    print(f"{count} film record(s) added successfully!")

# Function to fill the 'inventory' table with fake data
def fill_inventory_data(count):
    for _ in range(count):
        query = '''
        INSERT INTO inventory (film_id, last_update)
        VALUES (%s, %s)
        RETURNING inventory_id;
        '''
        values = (
            random.randint(1, 100),  # Assuming you have films with IDs from 1 to 100
            fake.date_time_this_decade()
        )
        cursor.execute(query, values)
        inventory_id = cursor.fetchone()[0]
        print(f"Inventory with ID {inventory_id} added successfully!")

    conn.commit()
    print(f"{count} inventory record(s) added successfully!")

# Function to fill the 'rental' table with fake data
def fill_rental_data(count):
    for _ in range(count):
        query = '''
        INSERT INTO rental (rental_date, inventory_id, customer_id, return_date, staff_id, last_update)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING rental_id;
        '''
        values = (
            fake.date_time_this_year(),
            random.randint(1, 100),  # Assuming you have inventory IDs from 1 to 100
            random.randint(1, 100),  # Assuming you have customer IDs from 1 to 100
            fake.date_time_this_year(),
            random.randint(1, 100),  # Assuming you have staff IDs from 1 to 100
            fake.date_time_this_decade()
        )
        cursor.execute(query, values)
        rental_id = cursor.fetchone()[0]
        print(f"Rental with ID {rental_id} added successfully!")

    conn.commit()
    print(f"{count} rental record(s) added successfully!")

# Function to fill the 'payment' table with fake data
def fill_payment_data(count):
    for _ in range(count):
        query = '''
        INSERT INTO payment (customer_id, staff_id, rental_id, amount, payment_date)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING payment_id;
        '''
        values = (
            random.randint(1, 100),  # Assuming you have customer IDs from 1 to 100
            random.randint(1, 100),  # Assuming you have staff IDs from 1 to 100
            random.randint(1, 100),  # Assuming you have rental IDs from 1 to 100
            round(random.uniform(1, 99.99), 2),
            fake.date_time_this_decade()
        )
        cursor.execute(query, values)
        payment_id = cursor.fetchone()[0]
        print(f"Payment with ID {payment_id} added successfully!")

    conn.commit()
    print(f"{count} payment record(s) added successfully!")


if __name__ == '__main__':
    fill_customer_data(100)  # Change the count as needed
    fill_staff_data(100)
    fill_film_data(100)
    fill_inventory_data(100)
    fill_rental_data(100)
    fill_payment_data(100)

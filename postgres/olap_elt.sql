SET search_path TO olap, public;

CREATE EXTENSION IF NOT EXISTS dblink;

CREATE OR REPLACE PROCEDURE olap.run_elt()
LANGUAGE plpgsql
AS $$
BEGIN

    -- Populate date_dim
    INSERT INTO olap.date_dim(date_id, date, year, month, day_of_month, week_of_year, day_of_week)
    SELECT TO_CHAR(date_seq, 'yyyymmdd')::INT AS date_id,
        date_seq AS date,
        EXTRACT(ISOYEAR FROM date_seq) AS year,
        EXTRACT(MONTH FROM date_seq) AS month,
        EXTRACT(DAY FROM date_seq) AS day_of_month,
        EXTRACT(WEEK FROM date_seq) AS week_of_year,
        EXTRACT(ISODOW FROM date_seq) AS day_of_week
    FROM (
        SELECT '2010-01-01'::DATE + SEQUENCE.DAY AS date_seq
        FROM GENERATE_SERIES(0, 5000) AS SEQUENCE(DAY)
        ORDER BY date_seq
    ) DS
    ON CONFLICT(date_id) DO NOTHING;

    -- Populate film_dim using dblink and type casting
    INSERT INTO olap.film_dim(film_id, title, release_year, rating)
    SELECT film_id, title, release_year, rating::mpaa_rating -- Assuming rating is of type mpaa_rating in film_dim
    FROM dblink('dbname=oltp_db host=oltp-db port=5432 user=postgres password=Root#123', 'SELECT film_id, title, release_year, rating FROM oltp.film') AS t(film_id INT, title VARCHAR, release_year INT, rating VARCHAR)
    ON CONFLICT(film_id) DO UPDATE SET 
        title = EXCLUDED.title,
        release_year = EXCLUDED.release_year,
        rating = EXCLUDED.rating::mpaa_rating; -- Assuming rating is of type mpaa_rating in film_dim

    -- Populate staff_dim using dblink
    INSERT INTO olap.staff_dim(staff_id, first_name, last_name)
    SELECT staff_id, first_name, last_name
    FROM dblink('dbname=oltp_db host=oltp-db port=5432 user=postgres password=Root#123', 'SELECT staff_id, first_name, last_name FROM oltp.staff') AS t(staff_id INT, first_name VARCHAR, last_name VARCHAR)
    ON CONFLICT(staff_id) DO UPDATE SET 
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name;

    -- Populate customer_dim using dblink
    INSERT INTO olap.customer_dim (customer_id, first_name, last_name)
    SELECT customer_id, first_name, last_name
    FROM dblink('dbname=oltp_db host=oltp-db port=5432 user=postgres password=Root#123', 'SELECT customer_id, first_name, last_name FROM oltp.customer') AS t(customer_id INT, first_name VARCHAR, last_name VARCHAR)
    ON CONFLICT(customer_id) DO UPDATE SET
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name;

    -- Create temporary table for sales_facts
    CREATE TABLE olap.sales_facts_tmp (LIKE olap.sales_facts INCLUDING ALL);

    -- Populate sales_facts_tmp using dblink and type casting
    INSERT INTO olap.sales_facts_tmp(
        film_id, customer_id, staff_id, date_id, amount
    ) SELECT DISTINCT i.film_id, 
                p.customer_id, 
                p.staff_id, 
                TO_CHAR(payment_date, 'yyyymmdd')::INT AS date_id, 
                amount
    FROM dblink('dbname=oltp_db host=oltp-db port=5432 user=postgres password=Root#123', 'SELECT film_id, customer_id, staff_id, payment_date, amount FROM oltp.payment p JOIN oltp.rental r ON p.rental_id = r.rental_id JOIN oltp.inventory i ON r.inventory_id = i.inventory_id') AS t(film_id INT, customer_id INT, staff_id INT, payment_date TIMESTAMP, amount NUMERIC)
    ON CONFLICT ON CONSTRAINT sales_facts_tmp_pkey DO NOTHING;

    -- Rename and replace tables
    ALTER TABLE olap.sales_facts RENAME TO sales_facts_old;
    ALTER TABLE olap.sales_facts_tmp RENAME TO sales_facts;
    
    -- Drop old table
    DROP TABLE olap.sales_facts_old CASCADE;

    -- Commit the transaction
    COMMIT;
END; 
$$;



CALL olap.run_elt();
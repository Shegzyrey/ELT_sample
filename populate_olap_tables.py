import psycopg2

# establishing the connection
conn = psycopg2.connect(
    database="postgres",
    user="postgres",
    password="Root123$",
    host="127.0.0.1",
    port="5432",
)
conn.autocommit = True

# Creating a cursor object using the cursor() method
cursor = conn.cursor()


def olap_batch_processing():
    # create INSERT SQL statement

    sql_trunc_dim_film = """
    TRUNCATE TABLE olap_schema.dim_film
    """
    cursor.execute(sql_trunc_dim_film)

    sql_pop_dim_film = """
    INSERT INTO olap_schema.dim_film
    SELECT DISTINCT film_id,
        title, 
        release_year, 
        length as duration,
        now() as last_update
    FROM oltp_schema.film
    """
    cursor.execute(sql_pop_dim_film)

    sql_trunc_dim_cust = """
    TRUNCATE TABLE olap_schema.dim_customer;
    """
    cursor.execute(sql_trunc_dim_cust)

    sql_pop_dim_cust = """
    INSERT INTO olap_schema.dim_customer
    SELECT customer_id, first_name ||' '|| last_name AS customer_name,
        email, create_date, 
        now() as last_update
    FROM oltp_schema.customer 
    """
    cursor.execute(sql_pop_dim_cust)

    sql_trunc_dim_staff = """
    TRUNCATE TABLE olap_schema.dim_staff;
    """
    cursor.execute(sql_trunc_dim_staff)

    sql_pop_dim_staff = """ 
        INSERT INTO olap_schema.dim_staff 
        SELECT DISTINCT staff_id, first_name ||' '|| last_name AS staff_name,
                email, now() as last_update
        FROM oltp_schema.staff
        """
    cursor.execute(sql_pop_dim_staff)

    sql_trunc_dim_date = """
    TRUNCATE TABLE olap_schema.dim_staff;
    """
    cursor.execute(sql_trunc_dim_date)

    sql_pop_dim_date = """
    INSERT INTO olap_schema.dim_date
    SELECT TO_CHAR(ax.payment_date, 'YYYYMMDD') date_id,
            CAST(TO_CHAR(ax.payment_date, 'YYYY-MM-DD') AS DATE) date_,
        TO_CHAR(ax.payment_date, 'YYYY') year_,
        TO_CHAR(ax.payment_date, 'Q') quarter_,
        TO_CHAR(ax.payment_date, 'MON') month_,
        TO_CHAR(ax.payment_date, 'DD') day_of_week,
        TO_CHAR(ax.payment_date, 'IW') week_of_year
    FROM oltp_schema.payment AX
    """
    cursor.execute(sql_pop_dim_date)

    sql_trunc_fact_txn = """
    TRUNCATE TABLE olap_schema.fact_transacion 
    """
    cursor.execute(sql_trunc_fact_txn)

    sql_pop_fact_txn = """
    INSERT INTO olap_schema.fact_transacion 
    SELECT DISTINCT bx.customer_id,
        cx.staff_id, dx.rental_id,
        TO_CHAR(ax.payment_date, 'YYYYMMDD') date_id,
        SUM(ax.amount) total_revenue,
        AVG(ax.amount) avg_revenue,
        NOW() as last_update
    FROM oltp_schema.payment AX
    LEFT JOIN oltp_schema.customer BX ON bx.customer_id = ax.customer_id
    LEFT JOIN oltp_schema.staff CX ON cx.staff_id = ax.customer_id
    LEFT JOIN oltp_schema.rental DX ON dx.rental_id = ax.customer_id
    GROUP BY ax.payment_id, bx.customer_id,
            cx.staff_id, dx.rental_id,
            TO_CHAR(ax.payment_date, 'YYYYMMDD')
    """
    cursor.execute(sql_pop_fact_txn)

    conn.commit()

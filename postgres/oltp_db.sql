CREATE TABLE IF NOT EXISTS customer (
    customer_id SERIAL NOT NULL,
    first_name character varying(45) NOT NULL,
    last_name character varying(45) NOT NULL,
    email character varying(50),
    create_date date DEFAULT ('now'::text)::date NOT NULL,
    last_update timestamp without time zone DEFAULT now(),
	PRIMARY KEY (customer_id)
);

CREATE TABLE IF NOT EXISTS staff (
    staff_id SERIAL NOT NULL,
    first_name character varying(45) NOT NULL,
    last_name character varying(45) NOT NULL,
    email character varying(50),
    last_update timestamp without time zone DEFAULT now() NOT NULL,
	PRIMARY KEY (staff_id)
);

CREATE TABLE IF NOT EXISTS film (
    film_id SERIAL NOT NULL,
    title character varying(255) NOT NULL,
    description text,
    release_year DATE NOT NULL DEFAULT CURRENT_DATE,
    original_language_id smallint,
    rental_duration smallint DEFAULT 3 NOT NULL,
    rental_rate numeric(4,2) DEFAULT 4.99 NOT NULL,
    length smallint,
    replacement_cost numeric(5,2) DEFAULT 19.99 NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL,
	PRIMARY KEY (film_id)
);

CREATE TABLE IF NOT EXISTS inventory (
    inventory_id SERIAL NOT NULL,
    film_id smallint NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL,
	PRIMARY KEY (inventory_id),
	FOREIGN KEY (film_id) REFERENCES film (film_id)
);

CREATE TABLE IF NOT EXISTS rental (
    rental_id SERIAL NOT NULL,
    rental_date timestamp without time zone NOT NULL,
    inventory_id INT NOT NULL,
    customer_id INT NOT NULL,
    return_date timestamp without time zone,
    staff_id INT NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL,
	PRIMARY KEY (rental_id),
	FOREIGN KEY (inventory_id) REFERENCES inventory (inventory_id),
	FOREIGN KEY (customer_id)  REFERENCES customer (customer_id),
	FOREIGN KEY (staff_id)     REFERENCES staff (staff_id)
);

CREATE TABLE IF NOT EXISTS payment (
    payment_id SERIAL NOT NULL,
    customer_id integer NOT NULL,
    staff_id integer NOT NULL,
    rental_id integer NOT NULL,
    amount numeric(5,2) NOT NULL,
    payment_date timestamp without time zone NOT NULL,
    PRIMARY KEY (payment_id),
	FOREIGN KEY (staff_id)     REFERENCES staff (staff_id),
 	FOREIGN KEY (customer_id)  REFERENCES customer (customer_id),
	FOREIGN KEY (rental_id)    REFERENCES rental (rental_id)
);
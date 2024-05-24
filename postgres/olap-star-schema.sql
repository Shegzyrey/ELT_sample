SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;


CREATE SCHEMA IF NOT EXISTS olap;

DROP TABLE IF EXISTS olap.customer_dim;
CREATE TABLE olap.customer_dim (
    customer_id INTEGER PRIMARY KEY,
    first_name VARCHAR(254),
    last_name VARCHAR(254)
);

CREATE TYPE olap.mpaa_rating AS ENUM (
    'G',
    'PG',
    'PG-13',
    'R',
    'NC-17'
);


ALTER TYPE olap.mpaa_rating OWNER TO postgres;

DROP TABLE IF EXISTS olap.film_dim;
CREATE TABLE olap.film_dim(
    film_id INTEGER PRIMARY KEY,
    title VARCHAR(254),
    release_year INTEGER,
    rating olap.mpaa_rating
);

DROP TABLE IF EXISTS olap.staff_dim;
CREATE TABLE olap.staff_dim(
    staff_id INTEGER PRIMARY KEY,
    first_name VARCHAR(254),
    last_name VARCHAR(254)
);

DROP TABLE IF EXISTS olap.date_dim;
CREATE TABLE olap.date_dim(
    date_id INTEGER PRIMARY KEY,
    date TIMESTAMP WITH TIME ZONE,
    year INTEGER,
    month INTEGER,
    day_of_month INTEGER,
    week_of_year INTEGER,
    day_of_week INTEGER
);

DROP TABLE IF EXISTS olap.sales_facts;
CREATE TABLE olap.sales_facts(
    film_id INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    staff_id INTEGER NOT NULL,
    date_id INTEGER NOT NULL,
    amount NUMERIC(5, 2),
    UNIQUE(film_id, customer_id, staff_id, date_id)
);


SET search_path TO olap, public;

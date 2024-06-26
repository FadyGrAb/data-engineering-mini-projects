-- Create Database
DROP DATABASE IF EXISTS retail;

CREATE Database retail;

\c retail;

-- Create tables
CREATE TABLE
    orders (
        orderid SERIAL PRIMARY KEY,
        ordertime TIMESTAMP NOT NULL,
        branch VARCHAR(50) NOT NULL
    );

CREATE TABLE
    ordershistory (
        historyid SERIAL PRIMARY KEY,
        orderid INT NOT NULL,
        status VARCHAR(10) NOT NULL,
        updatedat TIMESTAMP NOT NULL,
        FOREIGN KEY (orderid) REFERENCES orders (orderid)
    );

-- Create App User
CREATE USER appuser
WITH
    PASSWORD 'apppassword';

GRANT USAGE ON SCHEMA public TO appuser;

GRANT USAGE
    ON ALL SEQUENCES IN SCHEMA public TO appuser;

GRANT INSERT,
UPDATE, 
SELECT
    ON ALL TABLES IN SCHEMA public TO appuser;

-- Create Spark User
CREATE USER sparkuser WITH PASSWORD 'sparkpassword';
GRANT USAGE ON SCHEMA public TO sparkuser;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO sparkuser;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO sparkuser;

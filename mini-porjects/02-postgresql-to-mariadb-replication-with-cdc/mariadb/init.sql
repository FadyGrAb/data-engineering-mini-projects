CREATE DATABASE IF NOT EXISTS requests;

CREATE TABLE
    IF NOT EXISTS requests.itrequests (
        requestid INT PRIMARY KEY,
        details VARCHAR(60) NOT NULL,
        duedate DATE NOT NULL,
        fulfilled BOOLEAN NOT NULL
    );

-- Create user 'mariadb' and grant privileges
CREATE USER IF NOT EXISTS 'mariadb'@'%' IDENTIFIED BY 'mariadb';
GRANT ALL PRIVILEGES ON requests.* TO 'mariadb'@'%';

FLUSH PRIVILEGES;
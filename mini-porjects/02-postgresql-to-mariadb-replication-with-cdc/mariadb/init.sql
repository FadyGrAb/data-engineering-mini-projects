CREATE DATABASE IF NOT EXISTS requests;

CREATE TABLE
    IF NOT EXISTS requests.ITRequests (
        RequestID INT PRIMARY KEY,
        Details VARCHAR(60) NOT NULL,
        DueDate DATE NOT NULL,
        Fulfilled BOOLEAN NOT NULL
    );

-- Create user 'mariadb' and grant privileges
CREATE USER IF NOT EXISTS 'mariadb'@'%' IDENTIFIED BY 'mariadb';
GRANT ALL PRIVILEGES ON requests.* TO 'mariadb'@'%';

FLUSH PRIVILEGES;
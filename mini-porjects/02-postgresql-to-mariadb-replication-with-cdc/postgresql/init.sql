DROP TABLE IF EXISTS itrequests;

-- Create Test table
CREATE TABLE
    itrequests (
        requestid SERIAL PRIMARY KEY,
        details VARCHAR(60) NOT NULL,
        duedate DATE NOT NULL,
        fulfilled BOOLEAN NOT NULL
    );
DROP TABLE IF EXISTS itrequests;

-- Create Test table
CREATE TABLE
    itrequests (
        requestid SERIAL PRIMARY KEY,
        details VARCHAR(60) NOT NULL,
        duedate DATE NOT NULL,
        fulfilled BOOLEAN NOT NULL
    );

-- Insert rows
INSERT INTO
    itrequests (details, duedate, fulfilled)
VALUES
    ('Reset my user password', '2024-07-15', FALSE),
    ('Repair laptop', '2024-07-20', FALSE),
    ('Restart main server', '2024-07-25', FALSE);

-- Update records
UPDATE itrequests
SET
    fulfilled = TRUE
WHERE
    requestid = 2;

-- Delete records
DELETE FROM itrequests
WHERE
    requestid = 1;
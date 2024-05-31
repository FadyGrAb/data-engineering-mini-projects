DROP TABLE IF EXISTS ITRequests;

-- Create Test table
CREATE TABLE
    ITRequests (
        RequestID SERIAL PRIMARY KEY,
        Details VARCHAR(60) NOT NULL,
        DueDate DATE NOT NULL,
        Fulfilled BOOLEAN NOT NULL
    );

-- Insert rows
INSERT INTO
    ITRequests (Details, DueDate, Fulfilled)
VALUES
    ('Reset my user password', '2024-07-15', FALSE),
    ('Repair laptop', '2024-07-20', FALSE),
    ('Restart main server', '2024-07-25', FALSE);

-- Update records
UPDATE ITRequests
SET
    Fulfilled = TRUE
WHERE
    RequestID = 2;

-- Delete records
DELETE FROM ITRequests
WHERE
    RequestID = 1;
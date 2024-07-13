-- Create Spark User
CREATE USER rustuser
WITH
    PASSWORD 'rust';

GRANT USAGE ON SCHEMA public TO rustuser;

GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO rustuser;

GRANT
SELECT
    ON ALL TABLES IN SCHEMA public TO rustuser;
SELECT
    DATE(p.payment_date) AS payment_date,
    c.customer_id,
    p.amount,
    i.film_id
FROM payment AS p 
INNER JOIN customer AS c 
ON p.customer_id = c.customer_id
INNER JOIN rental AS r 
ON p.rental_id = r.rental_id
INNER JOIN inventory AS i 
ON r.inventory_id = i.inventory_id
LIMIT 5;

SELECT 
    film_id,
    title,
    release_year
FROM
    film
LIMIT 5;

SELECT
    customer_id,
    first_name || ' ' || last_name AS name,
    active
FROM
    customer
LIMIT 5;
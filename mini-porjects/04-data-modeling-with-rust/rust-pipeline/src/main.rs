use dotenv::dotenv;
use duckdb::{params, Connection, Result};
use sqlx::postgres::PgPoolOptions;
use sqlx::types::time::Date;
use sqlx::types::BigDecimal;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenv().ok();

    let database_url = std::env::var("DATABASE_URL").unwrap();
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    // Create DuckDB database and tables
    let mut conn = Connection::open("star_schema.duckdb")?;
    create_tables(&conn)?;

    // Extract, transform, and load data incrementally
    let dim_customers = get_dim_customers(&pool).await?;
    let dim_films = get_dim_films(&pool).await?;
    let fact_sales = get_fact_sales(&pool).await?;

    save_to_duckdb(&mut conn, dim_customers, dim_films, fact_sales)?;

    Ok(())
}

fn create_tables(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS dim_customers (
            customer_id INTEGER PRIMARY KEY,
            name TEXT,
            active BOOLEAN
        );

        CREATE TABLE IF NOT EXISTS dim_films (
            film_id INTEGER PRIMARY KEY,
            title TEXT,
            release_year INTEGER
        );

        CREATE TABLE IF NOT EXISTS fact_sales (
            sales_id INTEGER PRIMARY KEY,
            sale_date DATE,
            customer_id INTEGER,
            amount NUMERIC,
            film_id INTEGER
        );
        ",
    )
}

async fn get_dim_customers(pool: &sqlx::PgPool) -> Result<Vec<(i32, String, i32)>, Box<dyn Error>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            customer_id,
            first_name || ' ' || last_name as name,
            active
        FROM customer
        "#
    )
    .fetch_all(pool)
    .await?;

    let mut customers = Vec::new();
    for row in rows {
        customers.push((row.customer_id, row.name.unwrap(), row.active.unwrap()));
    }

    Ok(customers)
}

async fn get_dim_films(pool: &sqlx::PgPool) -> Result<Vec<(i32, String, i32)>, Box<dyn Error>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            film_id,
            title,
            release_year
        FROM film
        "#
    )
    .fetch_all(pool)
    .await?;

    let mut films = Vec::new();
    for row in rows {
        films.push((row.film_id, row.title, row.release_year.unwrap()));
    }

    Ok(films)
}

async fn get_fact_sales(
    pool: &sqlx::PgPool,
) -> Result<
    Vec<(
        i32,
        Date,
        i32,
        BigDecimal,
        i32,
    )>,
    Box<dyn Error>,
> {
    let rows = sqlx::query!(
        r#"
        SELECT
            p.payment_id AS sales_id,
            DATE(p.payment_date) AS sale_date,
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
        "#
    )
    .fetch_all(pool)
    .await?;

    let mut sales = Vec::new();
    for row in rows {
        sales.push((
            row.sales_id,
            row.sale_date.unwrap(),
            row.customer_id,
            row.amount,
            row.film_id,
        ));
    }

    Ok(sales)
}

fn save_to_duckdb(
    conn: &mut Connection,
    dim_customers: Vec<(i32, String, i32)>,
    dim_films: Vec<(i32, String, i32)>,
    fact_sales: Vec<(
        i32,
        sqlx::types::time::Date,
        i32,
        sqlx::types::BigDecimal,
        i32,
    )>,
) -> Result<()> {
    let tx = conn.transaction()?;

    // Upsert dim_customers
    let mut stmt = tx.prepare(
        "INSERT OR REPLACE INTO dim_customers (customer_id, name, active) VALUES (?, ?, ?)",
    )?;
    for customer in dim_customers {
        stmt.execute(params![customer.0, customer.1, customer.2,])?;
    }

    // Upsert dim_films
    let mut stmt = tx.prepare(
        "INSERT OR REPLACE INTO dim_films (film_id, title, release_year) VALUES (?, ?, ?)",
    )?;
    for film in dim_films {
        stmt.execute(params![film.0, film.1, film.2])?;
    }

    // Upsert fact_sales
    let mut stmt = tx.prepare("INSERT OR REPLACE INTO fact_sales (sales_id, sale_date, customer_id, amount, film_id) VALUES (?, ?, ?, ?, ?)")?;
    for sale in fact_sales {
        stmt.execute(params![
            sale.0,
            sale.1.to_string(),
            sale.2,
            sale.3.to_string(),
            sale.4
        ])?;
    }

    tx.commit()
}

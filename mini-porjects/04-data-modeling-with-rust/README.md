cd mini-porjects/04-data-modeling-with-rust
docker compose up postgres -d
bin/deploy
docker compose up pipeline -d

bin/get-duckdb

sudo chmod 777 pipeline-run/duckdb/pagila_sales.duckdb 

bin/duckdb pipeline-run/duckdb/pagila_sales.duckdb

[ms](https://techcommunity.microsoft.com/t5/azure-database-for-postgresql/change-data-capture-in-postgres-how-to-use-logical-decoding-and/ba-p/1396421)

[dev.to](https://dev.to/thiagosilvaf/how-to-use-change-database-capture-cdc-in-postgres-37b8)
[RabbitMQ](https://www.rabbitmq.com/tutorials/tutorial-one-python)

1. docker compose up -d
2. docker compose exec postgresqldb bash -c "pg_recvlogical -U postgres -d requests --slot slot_cdc --create-slot -P wal2json"
3. docker compose exec postgresqldb bash -c "pg_recvlogical -U postgres -d requests --slot slot_cdc --start -f - &"
4. SELECT * FROM pg_logical_slot_peek_changes('slot_cdc', NULL, NULL);
5. SELECT * FROM pg_logical_slot_get_changes('slot_cdc', NULL, NULL);
6. docker compose exec postgresqldb bash -c "pg_recvlogical -U postgres -d requests --slot slot_cdc --drop-slot"
7. docker compose exec postgresqldb bash -c "psql -U postgres -d requests < /tmp/db-changes-demo.sql"
mariadb -h localhost -P 3306 -u mariadb -pmariadb < /tmp/init.sql 

SELECT slot_name, slot_type, database, active
FROM pg_replication_slots;
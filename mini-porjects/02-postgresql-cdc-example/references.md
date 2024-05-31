[ms](https://techcommunity.microsoft.com/t5/azure-database-for-postgresql/change-data-capture-in-postgres-how-to-use-logical-decoding-and/ba-p/1396421)

[dev.to](https://dev.to/thiagosilvaf/how-to-use-change-database-capture-cdc-in-postgres-37b8)


1. docker compose up -d
2. docker cp init.sh postgresql:/tmp/
3. docker cp db-changes-demo.sql postgresql:/tmp/
4. pg_recvlogical -U postgres -d requests --slot slot_cdc --create-slot -P wal2json
5. pg_recvlogical -U postgres -d requests --slot slot_cdc --start
6. SELECT * FROM pg_logical_slot_peek_changes('slot_cdc', NULL, NULL);
7. SELECT * FROM pg_logical_slot_get_changes('slot_cdc', NULL, NULL);
8. pg_recvlogical -U postgres -d requests --slot slot_cdc --drop-slot

docker cp ./db-changes-demo.sql postgesql:/tmp/
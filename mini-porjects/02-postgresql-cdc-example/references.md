[ms](https://techcommunity.microsoft.com/t5/azure-database-for-postgresql/change-data-capture-in-postgres-how-to-use-logical-decoding-and/ba-p/1396421)

[dev.to](https://dev.to/thiagosilvaf/how-to-use-change-database-capture-cdc-in-postgres-37b8)

apt-get install postgresql-16-wal2json
pg_recvlogical -U postgres -d requests --slot slot_cdc --create-slot -P wal2json
SELECT * FROM pg_logical_slot_peek_changes('slot_cdc', NULL, NULL);
pg_recvlogical -U postgres -d requests --slot slot_cdc --drop-slot
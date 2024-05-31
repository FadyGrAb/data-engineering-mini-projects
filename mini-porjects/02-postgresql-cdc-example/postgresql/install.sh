apt-get update && apt-get upgrade
apt-get install postgresql-16 postgresql-16-wal2json -y
mkdir -p /var/lib/postgresql/data
chown -R postgres:postgres /var/lib/postgresql/data
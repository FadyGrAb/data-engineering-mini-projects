# Adding repo to apt-get
echo "deb http://deb.debian.org/debian bullseye-backports main" > /etc/apt/sources.list.d/backports.list
apt-get update

# Installing dependencies
apt-get install openjdk-17-jdk wget python3 jupyter-notebook -y

# Downloading Spark
wget https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
tar -xaf spark-3.5.1-bin-hadoop3.tgz
mv spark-3.5.1-bin-hadoop3/* .
rm -rf spark-3.5.1-bin-hadoop3.tgz spark-3.5.1-bin-hadoop3

# Downloading additional jars
mkdir jars
wget -O ./jars/postgresql-jdbc.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

# Clean up
apt-get clean
apt-get remove wget -y
rm -rf /var/lib/apt/lists/*
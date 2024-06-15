FROM debian:stable-slim

WORKDIR /spark
COPY install-spark.sh run-spark.sh /spark/
RUN ./install-spark.sh
ENV SPARK_HOME=/spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

EXPOSE 8080
EXPOSE 8081
EXPOSE 7077
EXPOSE 4040
EXPOSE 18080
EXPOSE 8888
EXPOSE 6066
FROM ubuntu:latest

COPY ./install.sh /tmp/install.sh
RUN /tmp/install.sh

EXPOSE 5432
USER postgres
RUN /usr/lib/postgresql/16/bin/initdb -D /var/lib/postgresql/data
CMD ["/usr/lib/postgresql/16/bin/postgres", "-D", "/var/lib/postgresql/data"]
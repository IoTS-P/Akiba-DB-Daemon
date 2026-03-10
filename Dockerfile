# docker build -t akiba_db_daemon:3.0.1 .

FROM ubuntu:24.04

RUN apt-get update --fix-missing
RUN apt-get install -y openjdk-21-jdk wget postgresql-16 pgbackrest sudo zip unzip
RUN apt-get clean

RUN useradd -m -s /bin/bash akiba && \
    usermod -aG sudo akiba && \
    echo "akiba ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers.d/akiba && \
    chmod 440 /etc/sudoers.d/akiba && \
    mkdir /akiba && \
    chown akiba:akiba /akiba

USER akiba
WORKDIR /home/akiba

COPY build/distributions/akiba_db_daemon-3.1.0.zip .
#RUN wget https://github.com/IoTS-P/Akiba/releases/download/3.1.0/akiba_db_daemon-3.1.0.zip
RUN unzip akiba_db_daemon-3.1.0.zip && \
    rm akiba_db_daemon-3.1.0.zip && \
    cd akiba_db_daemon-3.1.0 && \
    mkdir /akiba/backups && \
    mkdir /akiba/instances

WORKDIR /home/akiba/akiba_db_daemon-3.1.0

CMD ["./bin/akiba_db_daemon", "-c", "./resources/config.json"]

FROM python:3.10-slim

# Set application directory
WORKDIR /ihub-loader

# Copies current directory into the container at /ihub-loader
ADD . /ihub-loader

# ADD ./odbcinst.ini /etc/odbcinst.ini
# RUN apt-get install gcc
# RUN apt-get update
# RUN apt-get install -y tdsodbc unixodbc-dev
# RUN apt install unixodbc-bin -y
# RUN apt-get clean -y

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update && \
    apt-get -y install gcc mono-mcs libpq-dev

RUN which pg_config

RUN pip install -r requirements.txt

# Copy our code to workdir /ihub-loader inside container
COPY . /ihub-loader
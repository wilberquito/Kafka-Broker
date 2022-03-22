FROM python:3.10-slim

# Set application directory
WORKDIR /ihub-loader

# Copies current directory into the container at /ihub-loader
COPY . /ihub-loader

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update && \
    apt-get -y install gcc g++ mono-mcs libpq-dev unixodbc-dev

RUN pip install -r requirements.txt

CMD ["python", "./app/main.py"]

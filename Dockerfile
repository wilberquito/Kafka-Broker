FROM python:3.10-slim

RUN apt-get update && \
    apt-get -y install gcc mono-mcs && \
    rm -rf /var/lib/apt/lists/*

# Set application directory
WORKDIR /ihub-loader

# Install requirements
COPY requirements.txt /ihub-loader/requirements.txt
RUN pip install -r requirements.txt

# Copy our code to workdir /ihub-loader inside container
COPY . .
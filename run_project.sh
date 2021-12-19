#!/bin/bash
# Install Docker and Docker Compose
apt install docker -y
apt install docker-compose -y

# Build Python ETL App image
docker build -t python-etl-app ./etl_app

# Build MySQL image
docker build -t mysql-etl-app ./mysql

# Run Docker Compose
docker-compose up
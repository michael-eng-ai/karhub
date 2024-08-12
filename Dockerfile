# Base image for MySQL
FROM mysql:8.0

# Set environment variables for MySQL
ENV MYSQL_DATABASE=airflow
ENV MYSQL_USER=airflow
ENV MYSQL_PASSWORD=airflow
ENV MYSQL_ROOT_PASSWORD=1234

# Expose MySQL port
EXPOSE 3306

#COPY ./init.sql /docker-entrypoint-initdb.d/

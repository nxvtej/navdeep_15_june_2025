FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y postgresql postgresql-contrib && \
    apt-get clean

WORKDIR /app

COPY . .
COPY ./data /app/data


RUN pip install --no-cache-dir -r req.txt

ENV DATABASE_URL=postgresql://myuser:password@localhost:5432/store_monitoring_db
ENV PGDATA=/var/lib/postgresql/data

RUN service postgresql start && \
    su postgres -c "psql -c \"CREATE USER \\\"myuser\\\" WITH PASSWORD 'password';\"" && \
    su postgres -c "psql -c \"CREATE DATABASE store_monitoring_db OWNER \\\"myuser\\\";\""

    EXPOSE 8000 5432

CMD service postgresql start && python -m business.ingest_data && uvicorn app.main:app --host 0.0.0.0 --port 8000

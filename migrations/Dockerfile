# Используем минимальный образ Python
FROM python:3.11-slim

# Устанавливаем зависимости
RUN pip install psycopg2-binary
RUN pip install clickhouse-driver

# Копируем миграционный скрипт в контейнер
WORKDIR /app
COPY migrations.py /app/migrations.py

# Устанавливаем точку входа
ENTRYPOINT ["python", "migrations.py"]

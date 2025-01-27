import logging
import sys

import psycopg2

from clickhouse_driver import Client


# Параметры подключения к БД
DB_CONFIG_POSTGRES = {
    'dbname': 'airflow',
    'user': 'airflow',
    'password': 'airflow',
    'host': 'postgres',  # Имя сервиса PostgreSQL в Docker Compose
    'port': 5432
}

DB_CONFIG_CLICKHOUSE = {
    'host': 'clickhouse',  # Имя сервиса ClickHouse в Docker Compose
    'port': 9000,
    'user': 'default',
    'password': 'clickpass',
    'database': 'click'
}

# SQL-запросы для миграций
CREATE_TABLE_QUERY_POSTGRES = '''
CREATE TABLE IF NOT EXISTS tg_reviews (
    id SERIAL PRIMARY KEY,
    review_id UUID NOT NULL UNIQUE,
    user_name VARCHAR(255) NOT NULL,
    user_image TEXT,
    language VARCHAR(50),
    country VARCHAR(50),
    content TEXT,
    score SMALLINT NOT NULL,
    thumbs_up_count INT NOT NULL,
    review_created_version VARCHAR(50),
    created_at TIMESTAMP NOT NULL,
    reply_content TEXT,
    replied_at TIMESTAMP,
    app_version VARCHAR(50)
);
'''

DROP_TABLE_QUERY_POSTGRES = '''
    DROP TABLE IF EXISTS tg_reviews;
'''

CREATE_TABLE_QUERY_CLICKHOUSE = '''
CREATE TABLE IF NOT EXISTS tg_review_stats (
    id Int32,
    event_date Date,
    language String,
    reviews_count Int32,
    min_score Float64,
    avg_score Float64,
    max_score Float64,
    insert_date Date,
    insert_datetime DateTime
) ENGINE = MergeTree()
ORDER BY id;
'''

DROP_TABLE_QUERY_CLICKHOUSE = '''
    DROP TABLE IF EXISTS tg_review_stats;
'''


def execute_postgres_query(query):
    """Выполнение SQL-запроса."""
    try:
        conn = psycopg2.connect(**DB_CONFIG_POSTGRES)
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        cursor.close()
        conn.close()
        logging.info("Миграции к постгре выполнены успешно.")
    except Exception as e:
        logging.error(f"Ошибка при выполнении запроса к постгре: {e}", exc_info=True)

def execute_clickhouse_query(query):
    """Выполнение SQL-запроса для ClickHouse."""
    try:
        client = Client(**DB_CONFIG_CLICKHOUSE)
        client.execute(query)
        logging.info("Миграции к кликхаус выполнены успешно.")
    except Exception as e:
        logging.error(f"Ошибка при выполнении запроса в ClickHouse: {e}", exc_info=True)

def main():
    if len(sys.argv) != 2:
        logging.info("Использование: python migrations.py [up|down]")
        sys.exit(1)

    command = sys.argv[1].lower()

    if command == "up":
        logging.info("Применение миграций...")
        execute_postgres_query(CREATE_TABLE_QUERY_POSTGRES)
        execute_clickhouse_query(CREATE_TABLE_QUERY_CLICKHOUSE)
    elif command == "down":
        logging.info("Откат миграций...")
        execute_postgres_query(DROP_TABLE_QUERY_POSTGRES)
        execute_clickhouse_query(DROP_TABLE_QUERY_CLICKHOUSE)
    else:
        logging.info("Неизвестная команда. Используйте 'up' или 'down'.")
        sys.exit(1)


if __name__ == "__main__":
    main()

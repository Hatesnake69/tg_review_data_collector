countries = [
    "us", "ru",
    # "gb", "fr", "de", "it", "es", "br", "in", "cn", "jp", "kr", "ca", "au", "mx", "za", "ar", "tr", "id",
]
languages = ["en", "ru"]
app_id = "org.telegram.messenger"
DB_CONFIG = {
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",  # Имя сервиса PostgreSQL в Docker Compose
    "port": 5432,
}
limit=50000
DB_CONFIG_CLICKHOUSE = {
    'host': 'clickhouse',  # Имя сервиса ClickHouse в Docker Compose
    'port': 9000,
    'user': 'default',
    'password': 'clickpass',
    'database': 'click'
}
from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime

from common import generate_tg_reviews_stats, fetch_and_store_reviews

# Настроим DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 26),
    "retries": 1,
}

dag = DAG(
    "tg_google_play_review_stats",
    default_args=default_args,
    description="Получение отзывов из Google Play и запись в кликхаус статистики",
    schedule_interval="0 12 * * *",
    max_active_runs=1,
)


# Задачи DAG
fetch_reviews_task = PythonOperator(
    task_id="fetch_reviews", python_callable=fetch_and_store_reviews, dag=dag
)

write_tg_stats_task = PythonOperator(
        task_id='write_tg_stats_task',
        python_callable=generate_tg_reviews_stats,
        dag=dag
    )

fetch_reviews_task >> write_tg_stats_task

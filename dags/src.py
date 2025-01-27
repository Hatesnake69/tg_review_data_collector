import logging
import time
from collections import defaultdict
from datetime import datetime, date, timedelta
from statistics import mean

from clickhouse_driver import Client


import psycopg2
from google_play_scraper import reviews
from psycopg2.extras import RealDictCursor

from config import languages, app_id, DB_CONFIG, limit, DB_CONFIG_CLICKHOUSE, countries
from schemas import TgReview, TgReviewStat, TgReviewFromDb, TgReviewStatToRecord


def fetch_reviews_for_all_countries_and_languages(country: str) -> list[TgReview]:
    all_reviews = []
    for lang in languages:
        logging.info(f"Сбор отзывов для страны: {country}, язык: {lang}")
        continuation_token = None  # Токен для следующей страницы
        retries = 5
        while True:
            reviews_data, continuation_token = reviews(
                app_id,
                lang=lang,
                country=country,
                count=limit,
                continuation_token=continuation_token,
            )
            all_reviews.extend([TgReview(**review, language=lang, country=country) for review in reviews_data])
            logging.info(
                f"Загружено {len(reviews_data)} отзывов для {country} ({lang}). Всего: {len(all_reviews)}"
            )
            if not continuation_token:
                break
            elif retries <= 0:
                break
            elif not reviews_data:
                time.sleep(5)
                retries -= 1
                logging.info(f"Получен пустой ответ, ждем 5 сек")
            if len(reviews_data) != limit:
                logging.info(f"Размер reviews_data меньше лимита => отзывов больше нет")
                break
    return all_reviews

def remove_null_bytes(value):
    if isinstance(value, str):
        return value.replace("\x00", "")
    return value

def save_reviews_to_postgres(reviews_data: list[TgReview]) -> None:
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS tg_reviews (
            review_id UUID PRIMARY KEY,
            user_name VARCHAR(255) NOT NULL,
            user_image TEXT,
            content TEXT NOT NULL,
            score SMALLINT NOT NULL,
            thumbs_up_count INT NOT NULL,
            review_created_version VARCHAR(50),
            created_at TIMESTAMP NOT NULL,
            reply_content TEXT,
            replied_at TIMESTAMP,
            app_version VARCHAR(50)
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        insert_query = """
        INSERT INTO tg_reviews (
            review_id, user_name, user_image, language, country, content, score, thumbs_up_count,
            review_created_version, created_at, reply_content, replied_at, app_version
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (review_id) DO NOTHING;
        """
        for review in reviews_data:
            sanitized_data = (
                review.reviewId,
                remove_null_bytes(review.userName),
                remove_null_bytes(str(review.userImage)),
                remove_null_bytes(review.language),
                remove_null_bytes(review.country),
                remove_null_bytes(review.content),
                review.score,
                review.thumbsUpCount,
                remove_null_bytes(review.reviewCreatedVersion),
                review.at.isoformat(),
                remove_null_bytes(review.replyContent),
                review.repliedAt.isoformat() if review.repliedAt else None,
                remove_null_bytes(review.appVersion),
            )
            try:
                cursor.execute(insert_query, sanitized_data)
            except Exception as e:
                logging.warning(
                    f"Не удалось вставить отзыв {review.reviewId}: {e}"
                )
        conn.commit()
        logging.info("Все отзывы успешно сохранены в PostgreSQL.")
    except Exception as e:
        logging.error(f"Ошибка при записи в PostgreSQL: {e}", exc_info=True)

def fetch_and_store_reviews():
    for country in countries:
        reviews_data = fetch_reviews_for_all_countries_and_languages(country)
        save_reviews_to_postgres(reviews_data)

def get_last_tg_stat_record() -> TgReviewStat | None:
    query = """
        SELECT *
        FROM tg_review_stats
        ORDER BY event_date DESC, id DESC
        LIMIT 1;
    """
    try:
        client = Client(**DB_CONFIG_CLICKHOUSE)
        result = client.execute(query)
        if result:
            serialized_record = TgReviewStat(
                id=result[0][0],
                event_date=result[0][1],
                language=result[0][2],
                reviews_count=result[0][3],
                min_score=result[0][4],
                avg_score=result[0][5],
                max_score=result[0][6],
                insert_date=result[0][7],
                insert_datetime=result[0][8],
            )
            logging.info(f"Последняя запись: {serialized_record}")
            return serialized_record
        else:
            logging.info("Таблица пуста.")
            return None
    except Exception as e:
        logging.error(f"Ошибка при выполнении запроса в ClickHouse: {e}", exc_info=True)


def get_tg_reviews(date_parameter: date = None) -> list[TgReviewFromDb]:
    query = "SELECT * FROM tg_reviews"
    params = []
    if date_parameter:
        query += " WHERE created_at >= %s  AND created_at < %s"
        params.append(date_parameter)
    today = datetime.now().date()
    params.append(today)
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query, params)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        logging.info(f"rows: {len(rows)}")
        return [TgReviewFromDb(**row) for row in rows]
    except Exception as e:
        logging.error(f"Ошибка при выполнении запроса: {e}", exc_info=True)
        return []

def write_stat_to_clickhouse(stat: TgReviewStatToRecord) -> None:
    check_query = """
            SELECT COUNT(*)
            FROM tg_review_stats
            WHERE event_date = %(event_date)s
              AND language = %(language)s
              AND reviews_count = %(reviews_count)s
              AND avg_score = %(avg_score)s;
        """

    query = """
        INSERT INTO tg_review_stats (
            event_date,
            language,
            reviews_count,
            min_score,
            avg_score,
            max_score,
            insert_date,
            insert_datetime
        ) VALUES (
            %(event_date)s,
            %(language)s,
            %(reviews_count)s,
            %(min_score)s,
            %(avg_score)s,
            %(max_score)s,
            %(insert_date)s,
            %(insert_datetime)s
        );
    """
    try:
        params = stat.model_dump()
        client = Client(**DB_CONFIG_CLICKHOUSE)
        count = client.execute(check_query, params)[0][0]
        if count > 0:
            logging.info(f"Запись уже существует: {stat}")
            return
        client.execute(query, params)
        logging.info(f"Запись успешно вставлена: {stat}")
    except Exception as e:
        logging.error(f"Ошибка при вставке записи в ClickHouse: {e}", exc_info=True)

def group_reviews_by_language_and_date(
        reviews: list[TgReviewFromDb]
) -> dict[tuple[str, datetime], list[TgReviewFromDb]]:
    grouped_reviews = defaultdict(list)
    for review in reviews:
        language = review.language
        date = review.created_at.date()
        key = (language, date)
        grouped_reviews[key].append(review)

    return dict(grouped_reviews)

def transform_to_stats(reviews: list[TgReviewFromDb]) -> list[TgReviewStatToRecord]:
    review_groups = group_reviews_by_language_and_date(reviews)
    res = []
    for language, review_date in review_groups:
        reviews_chunk = review_groups[(language, review_date)]
        reviews_count = len(reviews_chunk)
        max_review = max([elem.score for elem in reviews_chunk])
        min_review = min([elem.score for elem in reviews_chunk])
        avg_review = mean([elem.score for elem in reviews_chunk])
        instance = TgReviewStatToRecord(
            event_date=review_date,
            language=language,
            reviews_count=reviews_count,
            min_score=min_review,
            avg_score=avg_review,
            max_score=max_review,
            insert_date=datetime.today().date(),
            insert_datetime=datetime.now(),
        )
        res.append(instance)
    return res


def generate_tg_reviews_stats():
    last_tg_review_record = get_last_tg_stat_record()
    if not last_tg_review_record:
        reviews = get_tg_reviews()
    else:
        reviews = get_tg_reviews(date_parameter=datetime.now().date()-timedelta(days=1))
    logging.info(len(reviews))
    stats = transform_to_stats(reviews)
    for stat in stats:
        write_stat_to_clickhouse(stat)
    logging.info("tg_review_stats recorded")

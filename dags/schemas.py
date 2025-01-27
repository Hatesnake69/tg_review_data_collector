from pydantic import BaseModel, HttpUrl
from typing import Optional
from datetime import datetime, date


class TgReview(BaseModel):
    reviewId: str
    userName: str
    userImage: HttpUrl
    language: str
    country: str
    content: Optional[str] = None
    score: int
    thumbsUpCount: int
    reviewCreatedVersion: Optional[str] = None
    at: datetime
    replyContent: Optional[str] = None
    repliedAt: Optional[datetime] = None
    appVersion: Optional[str] = None

class TgReviewFromDb(BaseModel):
    id: int
    review_id: str
    user_name: str
    user_image: HttpUrl
    language: str
    country: str
    content: Optional[str] = None
    score: int
    thumbs_up_count: int
    review_created_version: Optional[str] = None
    created_at: datetime
    reply_content: Optional[str] = None
    replied_at: Optional[datetime] = None
    app_version: Optional[str] = None


class TgReviewStat(BaseModel):
    id: int
    event_date: date
    language: str
    reviews_count: int
    min_score: float
    avg_score: float
    max_score: float
    insert_date: date
    insert_datetime: datetime

class TgReviewStatToRecord(BaseModel):
    event_date: date
    language: str
    reviews_count: int
    min_score: float
    avg_score: float
    max_score: float
    insert_date: date
    insert_datetime: datetime
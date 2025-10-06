import json
import os
import random
import uuid
from datetime import datetime
from logging import getLogger
from typing import Optional, List

import httpx
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from fastapi import FastAPI
from pydantic import BaseModel, Field
from starlette import status

app = FastAPI()

logger = getLogger(__name__)
# "movie-events:1:1,user-events:1:1,payment-events:1:1"
# MONOLITH_URL = os.environ.get("MONOLITH_URL")
#      PORT: 8082
KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9093")

async def send_one(topic: str, message):
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKERS)
    # Get cluster layout and initial topic/partition leadership information.
    await producer.start()
    try:
        # Produce message
        return await producer.send_and_wait(topic=topic, value=message.model_dump_json().encode())
    finally:
        # Wait for all pending messages to be delivered or expire
        await producer.stop()


async def consume(topic: str, offset: int):
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BROKERS,
    )
    await consumer.start()
    try:
        assigned_partitions = list(consumer.assignment())
        target_partition = assigned_partitions[0]
        consumer.seek(target_partition, offset)
        msg = await consumer.getone()
        return msg
        # ---------
    finally:
        await consumer.stop()


class MovieEvent(BaseModel):
    movie_id: int
    title: str
    action: str
    user_id: Optional[int] = None
    rating: Optional[float] = None
    genres: Optional[List[str]] = None
    description: Optional[str] = None


class UserEvent(BaseModel):
    user_id: int = Field(description="Идентификатор пользователя.")
    username: Optional[str] = Field(None, description="Имя пользователя (опционально).")
    email: Optional[str] = Field(None, description="Email пользователя (опционально).")
    action: str = Field(description="Действие пользователя.")
    timestamp: datetime = Field(description="Время события.")


class PaymentEvent(BaseModel):

    payment_id: int = Field(description="Идентификатор платежа.")
    user_id: int = Field(description="Идентификатор пользователя.")
    amount: float = Field(description="Сумма платежа.")
    status: str = Field(description="Статус платежа.")
    timestamp: datetime = Field(description="Дата и время платежа.")
    method_type: Optional[str] = Field(None, description="Тип метода оплаты (опционально).")


class Event(BaseModel):
    id: str
    type: str
    timestamp: datetime
    payload: MovieEvent | PaymentEvent | UserEvent


class EventResponse(BaseModel):

    status: str
    partition: int
    offset: int
    event: Event



@app.get("/api/events/health")
async def health_check():
    return {"status": "ok"}


@app.post(
    "/api/events/movie",
    status_code=status.HTTP_201_CREATED
)
async def get_movies(body: MovieEvent):
    topic = "movie-events"
    report = await send_one(topic, body)
    msg = await consume(topic, report.offset)
    event = Event(
        id=str(uuid.uuid4()),
        type=topic,
        timestamp=msg.timestamp,
        payload=MovieEvent(**json.loads(msg.value.decode()))
    )
    return EventResponse(
        status="success",
        partition=report.partition,
        offset=report.offset,
        event=event
        )


@app.post(
    "/api/events/user",
    status_code=status.HTTP_201_CREATED
)
async def get_user(body: UserEvent):
    topic = "user-events"
    report = await send_one(topic, body)
    msg = await consume(topic, report.offset)
    event = Event(
        id=str(uuid.uuid4()),
        type=topic,
        timestamp=msg.timestamp,
        payload=UserEvent(**json.loads(msg.value.decode()))
    )
    return EventResponse(
        status="success",
        partition=report.partition,
        offset=report.offset,
        event=event
        )


@app.post(
    "/api/events/payment",
    status_code=status.HTTP_201_CREATED
)
async def get_payment(body: PaymentEvent):
    topic = "payment-events"
    report = await send_one(topic, body)
    msg = await consume(topic, report.offset)
    event = Event(
        id=str(uuid.uuid4()),
        type=topic,
        timestamp=msg.timestamp,
        payload=PaymentEvent(**json.loads(msg.value.decode()))
    )
    return EventResponse(
        status="success",
        partition=report.partition,
        offset=report.offset,
        event=event
        )


if __name__ == "__main__":
    import uvicorn

    # app = create_app()
    # host = app.state.settings.APP_HOST
    # port = app.state.settings.APP_PORT

    uvicorn.run('main:app', port=8082)





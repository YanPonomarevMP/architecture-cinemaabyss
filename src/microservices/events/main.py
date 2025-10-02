import os
import random
from datetime import datetime
from logging import getLogger
from typing import Optional, List

import httpx
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

logger = getLogger(__name__)
# "movie-events:1:1,user-events:1:1,payment-events:1:1"
# MONOLITH_URL = os.environ.get("MONOLITH_URL")
#      PORT: 8082
KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092")

async def send_one(topic: str, message: bytes):
    producer = AIOKafkaProducer(
        bootstrap_servers="localhost:9092")
    # Get cluster layout and initial topic/partition leadership information
    await producer.start()
    try:
        # Produce message
        await producer.send_and_wait(topic=topic, value=message)
    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()


async def consume(topic: str):
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BROKERS,
        group_id="my-group")
    # Get cluster layout and join group `my-group`
    await consumer.start()
    try:
        # Consume messages
        async for msg in consumer:
            return msg
            # print("consumed: ", msg.topic, msg.partition, msg.offset,
            #       msg.key, msg.value, msg.timestamp)
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()


class MovieEvent(BaseModel):
    movie_id: int
    title: str
    action: str
    user_id: Optional[int] = None
    rating: Optional[float] = None
    genres: Optional[List[str]] = None
    description: Optional[str] = None


class Event(BaseModel):
    id: str
    type: str
    timestamp: datetime
    payload: MovieEvent


class EventResponse(BaseModel):

    status: str
    partition: int
    offset: int
    event: Event



@app.get("/api/events/health")
async def health_check():
    return {"status": "ok"}


@app.post("/api/events/movie", )
async def get_movies(body: MovieEvent):
    topic = "movie-events"
    await send_one(topic, body.model_dump_json().encode())
    msg = await consume(topic)
    return EventResponse(
        status="success",
        partition=msg.partition,
        offset=msg.offset,
        event=body
        )


if __name__ == "__main__":
    import uvicorn

    # app = create_app()
    # host = app.state.settings.APP_HOST
    # port = app.state.settings.APP_PORT

    uvicorn.run(app, port=8082)





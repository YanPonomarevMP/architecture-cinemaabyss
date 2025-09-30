import os
import random
from logging import getLogger

import httpx
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

logger = getLogger(__name__)

MONOLITH_URL = os.environ.get("MONOLITH_URL")
MOVIES_SERVICE_URL = os.environ.get("MOVIES_SERVICE_URL")
EVENTS_SERVICE_URL = os.environ.get("EVENTS_SERVICE_URL")
GRADUAL_MIGRATION = os.environ.get("GRADUAL_MIGRATION") == "true"
MOVIES_MIGRATION_PERCENT = int(os.environ.get("MOVIES_MIGRATION_PERCENT"))


class Movie(BaseModel):
    id: int
    title: str
    description: str
    genres: list[str]
    rating: float


class User(BaseModel):
    id: int
    username: str
    email: str


@app.get("/health")
async def health_check():
    return {"status": "ok"}


@app.get("/api/movies", response_model=list[Movie])
async def get_movies():
    url = f"{MONOLITH_URL}/api/movies"

    if GRADUAL_MIGRATION:
        random_number = random.randrange(1, 101)
        if random_number <= MOVIES_MIGRATION_PERCENT:
            url = f"{MOVIES_SERVICE_URL}/api/movies"

    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        return response.json()


@app.get("/api/users", response_model=list[User])
async def get_users():
    url = f"{MONOLITH_URL}/api/users"

    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        return response.json()

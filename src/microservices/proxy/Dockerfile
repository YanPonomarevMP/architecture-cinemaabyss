FROM python:3.12-slim

WORKDIR /app

COPY . .

RUN pip install poetry &&  \
    poetry config virtualenvs.in-project true && \
    poetry config virtualenvs.create true && \
    poetry config installer.max-workers 25 && \
    poetry install  --no-interaction --no-ansi --no-root && \
    poetry install --no-interaction --no-ansi --no-root

EXPOSE 8000

CMD ["poetry", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

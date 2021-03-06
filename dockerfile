ARG PYTHON_VERSION=3.5.6-slim-jessie

FROM python:${PYTHON_VERSION} AS base

WORKDIR /app

FROM base AS dependencies

COPY app/requirements.txt app/requirements.txt

RUN  pip install -r app/requirements.txt

FROM base

WORKDIR /app

COPY /app /app

COPY --from=dependencies /root/.cache /root/.cache

RUN pip install -r requirements.txt && rm -rf /root/.cache

ENV PYTHONUNBUFFERED=0

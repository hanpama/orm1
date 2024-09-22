FROM python:3.12.4-slim-bookworm

ENV PYTHONUNBUFFERED=1
ENV LANG=en_US.utf8

WORKDIR /usr/app

RUN apt-get update -y && apt-get install -y fswatch && apt-get clean
RUN pip install --no-cache-dir coverage pyright && pyright --version

ADD . /usr/app
RUN pip install -q ".[dev]"

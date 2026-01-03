FROM python:3.13-slim

RUN pip install -U pip

WORKDIR /api_tests

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

CMD pytest .

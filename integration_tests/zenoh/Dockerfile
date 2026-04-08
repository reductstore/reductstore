FROM python:3.13

RUN pip install -U pip

WORKDIR /zenoh_tests

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

CMD pytest .

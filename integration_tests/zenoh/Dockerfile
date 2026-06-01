FROM python:3.13@sha256:a922e65d7cd72025d709fe78f3847c4cf89cc69c6fe1b6902f1c4d39fe9af82e

RUN pip install -U pip

WORKDIR /zenoh_tests

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

CMD pytest .

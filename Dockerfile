FROM python:3.10 as base

WORKDIR /app

ADD requirements.txt .

RUN pip install -r requirements.txt

COPY . .

CMD exec /bin/bash -c "trap : TERM INT; sleep infinity & wait"

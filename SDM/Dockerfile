FROM python:3.9.5-alpine3.12

RUN mkdir -p /app
WORKDIR /app

COPY ./src/requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt

COPY ./src/ /app/

CMD [ "python", "./main.py" ]

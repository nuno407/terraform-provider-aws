FROM python:3.9.5-alpine3.12

RUN mkdir -p /app
WORKDIR /app

COPY ./src/requirements.txt /app/requirements.txt
RUN pip install --proxy=http://127.0.0.1:3128 -r requirements.txt

COPY ./src/ /app/

CMD [ "python", "./main.py" ]

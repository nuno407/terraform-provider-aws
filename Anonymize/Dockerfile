FROM python:3.7.3-alpine3.9

RUN mkdir -p /app
WORKDIR /app

COPY ./src/requirements.txt /app/requirements.txt
RUN pip install --proxy=http://127.0.0.1:3128 -r requirements.txt

COPY ./src/ /app/

CMD [ "python", "-u", "./main.py" ]

FROM 081962623310.dkr.ecr.eu-central-1.amazonaws.com/base-images:python3.9.5-alpine3.12

RUN mkdir -p /app
WORKDIR /app

# Install ffmpeg program (to convert .avi to .mp4)
RUN apk update
RUN apk upgrade
RUN apk add ffmpeg

RUN echo @testing http://nl.alpinelinux.org/alpine/edge/testing >> /etc/apk/repositories
# install RabbitMQ from testing branch
RUN apk add rabbitmq-server@testing

RUN touch /usr/lib/rabbitmq/.erlang.cookie \
 && echo your-favourite-erlang-cookie > /usr/lib/rabbitmq/.erlang.cookie \
 && chown rabbitmq:rabbitmq /usr/lib/rabbitmq/.erlang.cookie \
 && chmod 700 /usr/lib/rabbitmq/.erlang.cookie


# Copy required directories to docker container
COPY ./ACAPI/src/ /app/src/
COPY ./baseaws/ /app/package/

# Install BaseAws package (custom)
WORKDIR /app/package
RUN pip install .

# Install required modules
WORKDIR /app/src
RUN pip install -r requirements.txt

# Run AC_API python script
#CMD [ "python", "./api.py", ";", "python", "./main.py" ]
RUN ["chmod", "+x", "start_container.sh"]
CMD ["sh", "start_container.sh"]
FROM 081962623310.dkr.ecr.eu-central-1.amazonaws.com/base-images:python3.9.12-alpine3.15

# Run as non-root user and group
RUN addgroup -S -g 3000 docker-group && \
    adduser -S -D -G docker-group -u 1000 docker
USER 1000

COPY --chown=1000:3000 ./chc_ivschain/requirements.txt /app/src/requirements.txt
WORKDIR /app/src
RUN pip install -r requirements.txt

COPY --chown=1000:3000 ./baseaws/ /app/package/
WORKDIR /app/package
RUN pip install .

COPY --chown=1000:3000 ./basehandler/ /app/package/
WORKDIR /app/package
RUN pip install .

COPY --chown=1000:3000 ./chc_ivschain/src/ /app/src/

WORKDIR /app/src

ENTRYPOINT ["python", "main.py"]

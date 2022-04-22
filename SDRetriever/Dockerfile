FROM 081962623310.dkr.ecr.eu-central-1.amazonaws.com/base-images:python3.9.12-alpine3.15

WORKDIR /app

# Install ffmpeg program (to convert .avi to .mp4)
RUN apk update
RUN apk upgrade
RUN apk add ffmpeg

# Run as non-root user and group
RUN addgroup -S -g 3000 docker-group && adduser -S -D -G docker-group -u 1000 docker
USER 1000
# Copy required directories to docker container
COPY --chown=1000:3000 ./SDRetriever/src/ /app/src/
COPY --chown=1000:3000 ./baseaws/ /app/package/
#COPY --chown=1000:3000 ./SDRetriever/pandas-1.4.2-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl /app/package/
# Install BaseAws package (custom)
WORKDIR /app/package
#RUN pip --no-cache-dir install --upgrade pip setuptools wheel ipython
RUN pip install .
#RUN pip install pandas-1.4.2-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl
#RUN rm pandas-1.4.2-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl

# Install required modules
WORKDIR /app/src
RUN pip install -r requirements.txt
# Run SDRetriever python script
CMD ["sh", "start_container.sh"]

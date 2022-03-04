FROM 081962623310.dkr.ecr.eu-central-1.amazonaws.com/base-images:python3.9.5-alpine3.12

# Necessary to install regex library
RUN apk add build-base

RUN mkdir -p /app
WORKDIR /app

# Copy required directories to docker container
COPY ./Metadata/src/ /app/src/
COPY ./baseaws/ /app/package/

# Install BaseAws package (custom)
WORKDIR /app/package
RUN pip install .

# Install required modules
WORKDIR /app/src
RUN pip install -r requirements.txt

# Run metadata api+service script
RUN ["chmod", "+x", "start_container.sh"]
CMD ["sh", "start_container.sh"]
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



#### VOXEL 51 

# Your install token
ARG TOKEN

# The Teams client version to install, or "latest"
ARG TEAMS_VERSION=latest

RUN if [ "${TEAMS_VERSION}" = "latest" ]; then \
        pip --no-cache-dir install --index-url https://${TOKEN}@pypi.fiftyone.ai fiftyone; \
    else \
        pip --no-cache-dir install --index-url https://${TOKEN}@pypi.fiftyone.ai fiftyone==${TEAMS_VERSION}; \
    fi

ARG FIFTYONE_DATABASE_URI 

ENV ROOT_DIR=/fiftyone

ENV FIFTYONE_MEDIA_CACHE_DIR=${ROOT_DIR}/cache \
    FIFTYONE_DATABASE_DIR=${ROOT_DIR}/db \
    FIFTYONE_DEFAULT_DATASET_DIR=${ROOT_DIR}/default \
    FIFTYONE_DATASET_ZOO_DIR=${ROOT_DIR}/zoo/datasets \
    FIFTYONE_MODEL_ZOO_DIR=${ROOT_DIR}/zoo/models \
    #FIFTYONE_DATABASE_URI="mongodb://fiftyonedevcloud:llNzXprqdDi7Rdc0@dataexploration-dev-shard-00-00.ci6wy.mongodb.net:27017,dataexploration-dev-shard-00-01.ci6wy.mongodb.net:27017,dataexploration-dev-shard-00-02.ci6wy.mongodb.net:27017/?ssl=true&replicaSet=atlas-gimhss-shard-0&authSource=admin&retryWrites=true&w=majority" \
    FIFTYONE_DATABASE_URI=${FIFTYONE_DATABASE_URI} \
    FIFTYONE_DATABASE_VALIDATION=false


# Correction, so that Voxel doesn't break if the DB is already populated
RUN mkdir -p ${ROOT_DIR}/db
RUN echo '{"version": "0.14.3"}' > ${ROOT_DIR}/db/config.json

##### END Voxel51 code




# Run metadata api+service script
RUN ["chmod", "+x", "start_container.sh"]
CMD ["sh", "start_container.sh"]

#### VOXEL 51 

FROM 081962623310.dkr.ecr.eu-central-1.amazonaws.com/base-images:ubuntu-20.04

# Your install token

# The Python version to install
#ARG PYTHON_VERSION=3.8
ENV  PYTHON_VERSION=3.8

#
# Install system packages
#
RUN apt -y update \
    && apt -y --no-install-recommends install software-properties-common \
    && add-apt-repository -y ppa:deadsnakes/ppa \
    && apt -y update \
    && apt -y upgrade \
    && apt -y --no-install-recommends install tzdata \
    && TZ=Etc/UTC \
    && apt -y --no-install-recommends install \
        build-essential \
        ca-certificates \
        cmake \
        cmake-data \
        pkg-config \
        libcurl4 \
        libsm6 \
        libxext6 \
        libssl-dev \
        libffi-dev \
        libxml2-dev \
        libxslt1-dev \
        net-tools \
        zlib1g-dev \
        unzip \
        curl \
        wget \
        python${PYTHON_VERSION} \
        python${PYTHON_VERSION}-dev \
        python${PYTHON_VERSION}-distutils \
        ffmpeg \
        s3fs \
        awscli \ 
    && ln -s /usr/bin/python${PYTHON_VERSION} /usr/local/bin/python \
    && curl https://bootstrap.pypa.io/get-pip.py | python \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir /fiftyone


ARG TOKEN

RUN echo ${TOKEN}
RUN echo ' '
RUN echo ${FIFTYONE_DATABASE_URI}

# The Teams client 
RUN pip --no-cache-dir install --index-url https://${TOKEN}@pypi.fiftyone.ai fiftyone

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


#FROM 081962623310.dkr.ecr.eu-central-1.amazonaws.com/base-images:python3.9.5-alpine3.12

# Necessary to install regex library
RUN apt install -y build-base

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
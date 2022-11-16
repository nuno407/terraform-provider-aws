#### VOXEL 51

FROM 081962623310.dkr.ecr.eu-central-1.amazonaws.com/base-images:ubuntu-20.04

# The Python version to install
ENV  PYTHON_VERSION=3.9

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
    && ln -s /usr/local/lib/python${PYTHON_VERSION} /usr/local/lib/python \
    && curl https://bootstrap.pypa.io/get-pip.py | python \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir /fiftyone

ENV ROOT_DIR=/fiftyone

RUN pip --no-cache-dir install --upgrade pip setuptools wheel ipython

ARG TOKEN
ARG FIFTYONE_DATABASE_URI

# The Teams client version to install, or "latest"
#ARG TEAMS_APP_VERSION==0.1.0
#ARG TEAMS_VERSION==0.8.0
#RUN if [ "${TEAMS_APP_VERSION}" = "latest" ]; then \
#        pip --no-cache-dir install --index-url https://${TOKEN}@pypi.fiftyone.ai fiftyone fiftyone-teams-app; \
#    else \
#        pip --no-cache-dir install --index-url https://${TOKEN}@pypi.fiftyone.ai fiftyone==${TEAMS_VERSION} fiftyone-teams-app==${TEAMS_APP_VERSION}; \
#    fi

RUN --mount=type=secret,id=voxel51token TOKEN=$(cat /run/secrets/voxel51token) \
    && pip --no-cache-dir install --extra-index-url https://${TOKEN}@pypi.fiftyone.ai fiftyone fiftyone-teams-app

#
# Configure shared storage
#
# The name of the shared directory in the container that should be
# volume-mounted by users to persist data loaded into FiftyOne

ENV FIFTYONE_MEDIA_CACHE_DIR=${ROOT_DIR}/cache \
    FIFTYONE_DATABASE_DIR=${ROOT_DIR}/db \
    FIFTYONE_DEFAULT_DATASET_DIR=${ROOT_DIR}/default \
    FIFTYONE_DATASET_ZOO_DIR=${ROOT_DIR}/zoo/datasets \
    FIFTYONE_TEAMS_ORGANIZATION=org_rir9rpZD8xHZSqet \
    TOKEN=${TOKEN} \
    FIFTYONE_TEAMS_ORGANIZATION=org_rir9rpZD8xHZSqet \
    FIFTYONE_TEAMS_CLIENT_ID=TJGoiAiTY3OX8qZKJsNzkVgdvxSGvGL0 \
#     FIFTYONE_DATABASE_NAME=fiftyone-rc \
    FIFTYONE_DATABASE_ADMIN=true \
    FIFTYONE_MODEL_ZOO_DIR=${ROOT_DIR}/zoo/models \
    FIFTYONE_DATABASE_URI=${FIFTYONE_DATABASE_URI} \
    FIFTYONE_DATABASE_VALIDATION=false


# Correction, so that Voxel doesn't break if the DB is already populated
RUN mkdir ${ROOT_DIR}/db
#RUN echo '{"version": "0.14.3"}' > ${ROOT_DIR}/db/config.json


#Launch Voxel front-end and bind it to port 5151
CMD hypercorn fiftyone.teams.app:app --bind 0.0.0.0:5151

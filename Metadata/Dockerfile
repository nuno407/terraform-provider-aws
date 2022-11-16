#### VOXEL 51
FROM 081962623310.dkr.ecr.eu-central-1.amazonaws.com/base-images:ubuntu-20.04

# The Python version to install
ENV PYTHON_VERSION=3.9

#
# Install system packages
#
RUN apt -y update \
    && apt -y --no-install-recommends install software-properties-common \
    && add-apt-repository -y 'ppa:deadsnakes/ppa' \
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
    && rm -rf /var/lib/apt/lists/*


# Run as non-root user and group
RUN addgroup --system --gid 3000 docker-group && adduser --disabled-login --system --gid 3000 --uid 1000 docker
# create fiftyone folder
RUN mkdir /fiftyone
RUN chown 1000:3000 -R /fiftyone

USER 1000

ENV ROOT_DIR=/fiftyone

# Correction, so that Voxel doesn't break if the DB is already populated
RUN mkdir ${ROOT_DIR}/db
#RUN echo '{"version": "0.14.3"}' > ${ROOT_DIR}/db/config.json

RUN pip --no-cache-dir install --upgrade pip setuptools wheel ipython

#
# Configure Voxel parameters

ENV FIFTYONE_MEDIA_CACHE_DIR=${ROOT_DIR}/cache \
    FIFTYONE_DATABASE_DIR=${ROOT_DIR}/db \
    FIFTYONE_DEFAULT_DATASET_DIR=${ROOT_DIR}/default \
    FIFTYONE_DATASET_ZOO_DIR=${ROOT_DIR}/zoo/datasets \
    FIFTYONE_TEAMS_ORGANIZATION=org_rir9rpZD8xHZSqet \
#    FIFTYONE_DATABASE_NAME=fiftyone-rc \
    TOKEN=${TOKEN} \
    FIFTYONE_TEAMS_ORGANIZATION=org_rir9rpZD8xHZSqet \
    FIFTYONE_TEAMS_CLIENT_ID=TJGoiAiTY3OX8qZKJsNzkVgdvxSGvGL0 \
    FIFTYONE_DATABASE_ADMIN=true \
    FIFTYONE_MODEL_ZOO_DIR=${ROOT_DIR}/zoo/models \
    FIFTYONE_DATABASE_VALIDATION=false


##### END Voxel51 code

# Add base library
COPY --chown=1000:3000 ./base /app/base

# Copy requirements.txt
COPY --chown=1000:3000 ./Metadata/requirements.txt /app/Metadata/requirements.txt
WORKDIR /app/Metadata

# The Teams client version to install, or "latest"
ARG TEAMS_APP_VERSION==0.10.0
ARG TEAMS_VERSION==0.18.0
RUN if [ "${TEAMS_APP_VERSION}" = "latest" ]; then \
        --mount=type=secret,id=voxel51token TOKEN=$(cat /run/secrets/voxel51token) \
        && pip --no-cache-dir install -r requirements.txt --extra-index-url https://${TOKEN}@pypi.fiftyone.ai fiftyone fiftyone-teams-app; \
    else \
        --mount=type=secret,id=voxel51token TOKEN=$(cat /run/secrets/voxel51token) \
        && pip --no-cache-dir install -r requirements.txt --extra-index-url https://${TOKEN}@pypi.fiftyone.ai fiftyone==${TEAMS_VERSION} fiftyone-teams-app==${TEAMS_APP_VERSION}; \
    fi

#Previous code to install always the latest
#RUN --mount=type=secret,id=voxel51token,uid=1000 TOKEN=$(cat /run/secrets/voxel51token) \
#&& pip install -r requirements.txt --extra-index-url https://${TOKEN}@pypi.fiftyone.ai

# Copy application code
COPY --chown=1000:3000 ./Metadata/metadata/ /app/Metadata/metadata
COPY --chown=1000:3000 ./Metadata/setup.py /app/Metadata/setup.py

WORKDIR /app/Metadata
RUN pip install -e .

COPY ./Metadata/entrypoint.sh /app/

ENTRYPOINT ["/app/entrypoint.sh"]

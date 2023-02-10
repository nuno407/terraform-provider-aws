FROM 081962623310.dkr.ecr.eu-central-1.amazonaws.com/base-images:python3.9.16-bullseye

# Install latest patches
RUN apt -y update && apt -y upgrade

# Run as non-root user and group
RUN addgroup --system --gid 3000 docker-group && adduser --disabled-login --system --gid 3000 --uid 1000 docker
USER 1000

# Add base library
COPY --chown=1000:3000 ./base /app/base

# Copy requirements.txt
COPY --chown=1000:3000 ./MDFParser/requirements.txt /app/mdfparser/requirements.txt
WORKDIR /app/mdfparser
RUN pip install -r requirements.txt

# Copy application code
COPY --chown=1000:3000 ./MDFParser/setup.py /app/mdfparser/setup.py
COPY --chown=1000:3000 ./MDFParser/mdfparser/ /app/mdfparser/mdfparser

WORKDIR /app/mdfparser
RUN pip install -e .

# Run mdfparser main by its package name
ENTRYPOINT [ "python", "-m", "mdfparser.main" ]

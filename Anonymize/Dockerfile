FROM python:3.9.5-alpine3.12

RUN mkdir -p /app
WORKDIR /app

# Copy required directories to docker container
COPY ./Anonymize/src/ /app/src/
COPY ./BaseAws/ /app/package/

# Install BaseAws package (custom)
WORKDIR /app/package
RUN pip install .

# Install required modules
WORKDIR /app/src
RUN pip install -r requirements.txt

# Run anonymize python script
CMD [ "python", "./main.py" ]
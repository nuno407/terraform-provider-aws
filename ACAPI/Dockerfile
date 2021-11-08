FROM 081962623310.dkr.ecr.eu-central-1.amazonaws.com/base-images:python3.9.5-alpine3.12

RUN mkdir -p /app
WORKDIR /app

# Install ffmpeg program (to convert .avi to .mp4)
RUN apt-get -y update
RUN apt-get -y upgrade
RUN apt-get install -y ffmpeg

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

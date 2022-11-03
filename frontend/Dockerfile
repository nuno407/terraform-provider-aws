# Stage 1: Compile and Build angular codebase
# Use official node image as the base image
FROM 081962623310.dkr.ecr.eu-central-1.amazonaws.com/base-images:node18-bullseye as build

# Set the working directory
WORKDIR /home/node/app

# use non-root user
RUN chown 1000:1000 ./
USER 1000

RUN npm config set registry https://artifactory.boschdevcloud.com/artifactory/api/npm/lab000003-bci-npm-virtual/
ARG NPM_AUTH
ARG NPM_EMAIL
COPY --chown=1000 frontend/.npmrc.docker .npmrc
COPY --chown=1000 frontend/package.json frontend/package-lock.json* ./
RUN npm ci && npm cache clean --force || cat /home/node/.npm/_logs/*debug-0.log

COPY --chown=1000 frontend/src ./src
COPY --chown=1000 frontend/angular.json frontend/tsconfig* ./
RUN npm run build

# Stage 2: Serve app with nginx server
# Use official unprivileged nginx image as the base image
FROM 081962623310.dkr.ecr.eu-central-1.amazonaws.com/base-images:nginx-unprivileged-1.23-alpine

USER root
# Install latest patches
RUN apk update && apk upgrade

# Use nginx non-privileged user
USER 101

# Copy the build output to replace the default nginx contents.
COPY frontend/nginx.conf /etc/nginx/nginx.conf
COPY --chown=101 --from=build /home/node/app/dist/ /usr/share/nginx/html

# Expose port 80
EXPOSE 80

# Inject environment variables on container start
CMD ["/bin/sh",  "-c",  "envsubst < /usr/share/nginx/html/assets/env.template.js > /usr/share/nginx/html/assets/env.js && exec nginx -g 'daemon off;'"]

# Stage 1: Compile and Build angular codebase
# Use official node image as the base image
FROM 081962623310.dkr.ecr.eu-central-1.amazonaws.com/base-images:node14-alpine3.12 as build

# Set the working directory
WORKDIR /home/node/app

# use non-root user
RUN chown node:node ./
USER node

RUN npm config set registry https://artifactory.boschdevcloud.com/artifactory/api/npm/lab000003-bci-npm-virtual/
ARG NPM_AUTH
ARG NPM_EMAIL
COPY .npmrc.docker .npmrc
COPY package.json package-lock.json* ./
RUN npm ci && npm cache clean --force

COPY ./src ./src
COPY angular.json tsconfig* ./
RUN npm run build

# Stage 2: Serve app with nginx server
# Use official unprivileged nginx image as the base image
FROM 081962623310.dkr.ecr.eu-central-1.amazonaws.com/base-images:nginx-unprivileged-1.21.6-alpine

# Use nginx non-privileged user
USER 101

# Copy the build output to replace the default nginx contents.
COPY nginx.conf /etc/nginx/nginx.conf
COPY --chown=101 --from=build /home/node/app/dist/ /usr/share/nginx/html

# Expose port 80
EXPOSE 80

# Inject environment variables on container start
CMD ["/bin/sh",  "-c",  "envsubst < /usr/share/nginx/html/assets/env.template.js > /usr/share/nginx/html/assets/env.js && exec nginx -g 'daemon off;'"]
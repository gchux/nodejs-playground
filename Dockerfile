FROM node:18.16.0-alpine3.17
RUN mkdir -p /opt/app
WORKDIR /opt/app
COPY package.json package-lock.json .
RUN npm install
COPY ./index.js .
EXPOSE 8080
CMD [ "npm", "start"]

FROM node:20-alpine

WORKDIR /app

RUN apk add --no-cache python3 make g++
ENV PYTHON=/usr/bin/python3

COPY package*.json ./
RUN npm ci --omit=dev && npm cache clean --force

COPY . .

EXPOSE 3000

USER node

CMD ["npm", "start"]

FROM node:20.15.1-alpine


WORKDIR /home/node/app

ENV NODE_ENV production

RUN apk --no-cache add bash curl dumb-init

COPY package*.json ./

RUN mkdir -p /home/node/app/.npm && \
    chown -R node:node /home/node/app && \
    npm config set cache /home/node/app/.npm && \
    npm ci --only=production

USER node

COPY --chown=node:node . .

CMD [ "node", "index.js" ]

FROM node:20-alpine

WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
RUN npm run build

# Copy static assets to standalone directory so they can be served by the server
RUN cp -r .next/static .next/standalone/.next/static
RUN cp -r public .next/standalone/public || true

EXPOSE 3000

ENV HOSTNAME="0.0.0.0"
CMD ["node", ".next/standalone/server.js"]

FROM node:22-alpine
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
EXPOSE 3002 50051
CMD ["npx", "nodemon", "index.js"]

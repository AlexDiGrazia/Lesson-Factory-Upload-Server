FROM --platform=linux/amd64 node:18   
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
ARG DATABASE_URL
ENV DATABASE_URL=$DATABASE_URL
RUN npx prisma generate
RUN npm run build
EXPOSE 3000
CMD npx prisma migrate deploy && npm run start:prod 
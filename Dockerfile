FROM --platform=linux/amd64 node:20.5.0
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
ARG DATABASE_URL
ENV DATABASE_URL=$DATABASE_URL
RUN npx prisma generate
RUN npm run build
EXPOSE 3001
CMD npm run start:prod 
FROM oven/bun:1.1-alpine

WORKDIR /app

COPY package.json bun.lockb* ./

RUN bun install

COPY . .
RUN bun run build

ENV NODE_ENV=production

EXPOSE 3000
#USER node
CMD ["bun", "run", "dist/index.js"]
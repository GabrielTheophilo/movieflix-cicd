# Imagem da aplicação Node.js
FROM node:20-alpine AS base

WORKDIR /app

# Instala dependências com cache eficiente
COPY app/package*.json ./
RUN npm install

# Copia o restante do app
COPY app/ ./

# Cria e ajusta permissões do volume de dados
RUN mkdir -p /data && chown -R node:node /data

USER node
EXPOSE 3000

VOLUME ["/data"]

CMD ["node", "server.js"]
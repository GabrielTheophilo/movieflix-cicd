FROM postgres:15-alpine

# Defina envs de usuário e banco
ENV POSTGRES_USER=movieflix \
    POSTGRES_PASSWORD=movieflix \
    POSTGRES_DB=movieflix_dw

# Copia script de inicialização
COPY docker/postgres/init.sql /docker-entrypoint-initdb.d/init.sql
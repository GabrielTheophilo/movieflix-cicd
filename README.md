# MovieFlix Analytics

## O que é
Projeto final que integra:
- App web para cadastrar filmes e avaliações.
- Nginx como proxy reverso.
- Pipeline CI/CD no GitHub Actions para build, teste e publicação.
- Fluxo de dados com Data Lake, Data Warehouse (DW) e Data Mart.
- ETL para carregar e preparar dados no PostgreSQL.


SQL para consultas do Data Mart -> arquivo analytics.sql

## Componentes
- App Web
  - Porta interna 3000.
  - Conecta no PostgreSQL.
- Nginx
  - Proxy reverso para o App.
  - Porta 80.
- PostgreSQL 15
  - Armazena DW e Data Marts.
- ETL
  - Lê CSVs/API, transforma e carrega no DW.
- Data Lake
  - Diretório com CSVs brutos: `movies.csv`, `users.csv`, `ratings.csv`.
- Rede Docker
  - Comunicação entre os contêineres.

## Fluxo de dados
- Data Lake: arquivos brutos (CSV).
- DW (PostgreSQL): tabelas tratadas (`movies`, `users`, `ratings`).
- Data Mart: visões com métricas para análise.
- ETL: carrega CSVs para DW e materializa Data Marts.

## Pipeline CI/CD (GitHub Actions)
- Build das imagens Docker (app, nginx, etl).
- Subida do PostgreSQL para testes.
- Teste rápido: app e nginx iniciam e respondem.
- Execução do ETL contra o banco.
- Push da imagem do app no Docker Hub (tag `latest` e SHA curto).
- Dica: criar o schema no banco antes do ETL (executar load_data.sql para criação das tabelas).

## Execução local (resumo)
- Criar rede: `docker network create movieflix-net`
- Subir PostgreSQL com variáveis e volume `data-lake`.
- Build e run do app e nginx.
- Colocar CSVs no diretório `data-lake`.
- Rodar o contêiner do ETL.

## Comandos para rodar localmente

Criar rede:
```bash
docker network create movieflix-net
```

Build do App:

```bash
docker build -f docker/app.Dockerfile -t gabrieltheophilo/movieflix-app:dev .
```
Rodar App:
```bash
docker run -d --name movieflix-app --network movieflix-net -p 3000:3000 -v "$(pwd)/data-lake:/data-lake" gabrieltheophilo/movieflix-app:dev
```

Subir PostgreSQL:
```bash
docker run -d --name movieflix-postgres --network movieflix-net -e POSTGRES_USER=movieflix -e POSTGRES_PASSWORD=movieflix -e POSTGRES_DB=movieflix_dw -p 5432:5432 -v "$(pwd)/data-lake:/data-lake" movieflix-postgres:dev
```

Build do ETL:

```bash
docker build -f docker/etl.Dockerfile -t movieflix-etl:dev .
```
Rodar ETL:

```bash
docker run --rm \
  --name movieflix-etl \
  --network movieflix-net \
  -e POSTGRES_HOST=movieflix-postgres \
  -e POSTGRES_USER=movieflix \
  -e POSTGRES_PASSWORD=movieflix \
  -e POSTGRES_DB=movieflix_dw \
  -v "$(pwd)/data-lake:/data-lake" \
  movieflix-etl:dev
```
Build do Nginx:

```bash
docker build -f docker/nginx/nginx.Dockerfile -t movieflix-nginx:dev .
```
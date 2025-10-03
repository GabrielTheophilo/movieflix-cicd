FROM python:3.11-slim

WORKDIR /app

# Instala dependências do sistema (necessárias para psycopg2)
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copia requirements e instala dependências Python
COPY etl/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o código do ETL
COPY etl/etl.py .

# Cria diretório para o Data Lake (será montado via volume)
RUN mkdir -p /data-lake

CMD ["python", "etl.py"]
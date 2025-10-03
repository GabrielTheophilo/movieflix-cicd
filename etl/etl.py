import pandas as pd
import unicodedata
import os
import time
from sqlalchemy import create_engine, text

# === Configuracao do PostgreSQL ===
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'movieflix-postgres')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'movieflix')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'movieflix')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'movieflix_dw')

# Caminho do Data Lake (montado via volume)
DATA_LAKE_DIR = os.getenv('DATA_LAKE_DIR', '/data-lake')

# Connection string
DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# === Funcao auxiliar: normalizar nomes de colunas ===
def normalize_col(col):
    """Remove espacos, acentos e caracteres especiais das colunas"""
    col = col.strip()
    col = unicodedata.normalize('NFKD', col).encode('ASCII', 'ignore').decode('ASCII')
    col = col.replace(' ', '_').replace('-', '_')
    return col.lower()

# === Funcao: aguardar PostgreSQL estar pronto ===
def wait_for_postgres(engine, max_retries=10, delay=3):
    """Aguarda o PostgreSQL estar pronto para conexoes"""
    print(f"[INFO] Aguardando PostgreSQL em {POSTGRES_HOST}:{POSTGRES_PORT}...")
    
    for attempt in range(max_retries):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("[OK] PostgreSQL esta pronto!")
            return True
        except Exception as e:
            print(f"   Tentativa {attempt + 1}/{max_retries} falhou: {e}")
            time.sleep(delay)
    
    raise Exception("[ERRO] PostgreSQL nao ficou disponivel a tempo")

# === Funcao: limpar tabelas ===
def truncate_tables(engine):
    """Limpa as tabelas antes de carregar novos dados"""
    print("[INFO] Limpando tabelas (TRUNCATE CASCADE)...")
    
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE ratings, movies, users RESTART IDENTITY CASCADE;"))
        conn.commit()
    
    print("[OK] Tabelas limpas.")

# === ETL: Movies ===
def etl_movies(engine):
    """Extrai, transforma e carrega dados de filmes"""
    file_path = os.path.join(DATA_LAKE_DIR, 'filmes.csv')
    print(f"\n[ETL] Processando movies de {file_path}...")
    
    # Extract
    df = pd.read_csv(file_path)
    print(f"   [INFO] {len(df)} registros extraidos")
    
    # Transform: normalizar colunas
    df.columns = [normalize_col(c) for c in df.columns]
    
    # Garantir que as colunas esperadas existem
    df = df.rename(columns={
        'id': 'id',
        'title': 'title',
        'genre': 'genre',
        'year': 'year'
    })
    
    # Tratar valores nulos e tipos
    df['id'] = df['id'].astype(int)
    df['title'] = df['title'].str.strip()
    df['genre'] = df['genre'].fillna('Unknown').str.strip()
    df['year'] = df['year'].fillna(0).astype(int)
    
    # Remover duplicatas
    df = df.drop_duplicates(subset=['id'])
    
    print(f"   [INFO] {len(df)} registros apos limpeza")
    
    # Load
    df.to_sql('movies', engine, if_exists='append', index=False)
    print(f"[OK] {len(df)} filmes carregados no DW")

# === ETL: Users ===
def etl_users(engine):
    """Extrai, transforma e carrega dados de usuarios"""
    file_path = os.path.join(DATA_LAKE_DIR, 'users.csv')
    print(f"\n[ETL] Processando users de {file_path}...")
    
    # Extract
    df = pd.read_csv(file_path)
    print(f"   [INFO] {len(df)} registros extraidos")
    
    # Transform: normalizar colunas
    df.columns = [normalize_col(c) for c in df.columns]
    
    df = df.rename(columns={
        'id': 'id',
        'name': 'name',
        'age': 'age',
        'country': 'country'
    })
    
    # Tratar valores nulos e tipos
    df['id'] = df['id'].astype(int)
    df['name'] = df['name'].str.strip()
    df['age'] = df['age'].fillna(0).astype(int)
    df['country'] = df['country'].fillna('Unknown').str.strip()
    
    # Remover duplicatas
    df = df.drop_duplicates(subset=['id'])
    
    print(f"   [INFO] {len(df)} registros apos limpeza")
    
    # Load
    df.to_sql('users', engine, if_exists='append', index=False)
    print(f"[OK] {len(df)} usuarios carregados no DW")

# === ETL: Ratings ===
def etl_ratings(engine):
    """Extrai, transforma e carrega dados de avaliacoes"""
    file_path = os.path.join(DATA_LAKE_DIR, 'ratings.csv')
    print(f"\n[ETL] Processando ratings de {file_path}...")
    
    # Extract
    df = pd.read_csv(file_path)
    print(f"   [INFO] {len(df)} registros extraidos")
    
    # Transform: normalizar colunas
    df.columns = [normalize_col(c) for c in df.columns]
    
    df = df.rename(columns={
        'id': 'id',
        'user_id': 'user_id',
        'movie_id': 'movie_id',
        'score': 'score'
    })
    
    # Tratar valores nulos e tipos
    df['id'] = df['id'].astype(int)
    df['user_id'] = df['user_id'].astype(int)
    df['movie_id'] = df['movie_id'].astype(int)
    df['score'] = df['score'].astype(int)
    
    # Validar score (1-5)
    df = df[(df['score'] >= 1) & (df['score'] <= 5)]
    
    # Remover duplicatas
    df = df.drop_duplicates(subset=['id'])
    
    print(f"   [INFO] {len(df)} registros apos limpeza")
    
    # Load
    df.to_sql('ratings', engine, if_exists='append', index=False)
    print(f"[OK] {len(df)} avaliacoes carregadas no DW")

# === Criar views do Data Mart ===
def create_data_mart_views(engine):
    """Cria views agregadas para analises (Data Mart)"""
    print("\n[INFO] Criando views do Data Mart...")
    
    views = {
        'top_movies': """
            CREATE OR REPLACE VIEW top_movies AS
            SELECT m.title, m.genre, ROUND(AVG(r.score)::numeric,2) as avg_score, COUNT(r.id) as total_ratings
            FROM movies m
            JOIN ratings r ON m.id = r.movie_id
            GROUP BY m.title, m.genre
            ORDER BY avg_score DESC, total_ratings DESC
            LIMIT 10;
        """,
        'avg_rating_by_country': """
            CREATE OR REPLACE VIEW avg_rating_by_country AS
            SELECT u.country, ROUND(AVG(r.score)::numeric,2) as avg_score, COUNT(r.id) as total_ratings
            FROM ratings r
            JOIN users u ON u.id = r.user_id
            GROUP BY u.country
            ORDER BY avg_score DESC;
        """,
        'avg_rating_by_age_group': """
            CREATE OR REPLACE VIEW avg_rating_by_age_group AS
            SELECT
              CASE
                WHEN age < 20 THEN 'under 20'
                WHEN age BETWEEN 20 AND 29 THEN '20s'
                WHEN age BETWEEN 30 AND 39 THEN '30s'
                WHEN age BETWEEN 40 AND 49 THEN '40s'
                ELSE '50+'
              END as age_group,
              ROUND(AVG(r.score)::numeric,2) as avg_score,
              COUNT(r.id) as total_ratings
            FROM ratings r
            JOIN users u ON u.id = r.user_id
            GROUP BY age_group
            ORDER BY avg_score DESC;
        """,
        'best_genre': """
            CREATE OR REPLACE VIEW best_genre AS
            SELECT m.genre, ROUND(AVG(r.score)::numeric,2) as avg_score, COUNT(r.id) as total_ratings
            FROM movies m
            JOIN ratings r ON m.id = r.movie_id
            GROUP BY m.genre
            ORDER BY avg_score DESC, total_ratings DESC
            LIMIT 1;
        """
    }
    
    with engine.connect() as conn:
        for view_name, view_sql in views.items():
            conn.execute(text(view_sql))
            print(f"   [OK] View '{view_name}' criada")
        conn.commit()
    
    print("[OK] Views do Data Mart criadas com sucesso")

# === Executar consultas analiticas ===
def run_analytics(engine):
    """Executa consultas analiticas e exibe resultados"""
    print("\n=== CONSULTAS ANALITICAS ===\n")
    
    queries = {
        "Top 10 Filmes": "SELECT * FROM top_movies;",
        "Melhor Genero": "SELECT * FROM best_genre;",
        "Pais com mais avaliacoes": """
            SELECT country, COUNT(r.id) as total_reviews
            FROM ratings r
            JOIN users u ON r.user_id = u.id
            GROUP BY country
            ORDER BY total_reviews DESC
            LIMIT 1;
        """,
        "Media por faixa etaria": "SELECT * FROM avg_rating_by_age_group;"
    }
    
    with engine.connect() as conn:
        for title, query in queries.items():
            print(f"\n[QUERY] {title}:")
            df = pd.read_sql(query, conn)
            print(df.to_string(index=False))
            print()

# === Funcao principal ===
def main():
    print("[INICIO] Iniciando ETL MovieFlix (Python + Pandas)...\n")
    
    try:
        # Criar engine do SQLAlchemy
        engine = create_engine(DATABASE_URL)
        
        # Aguardar PostgreSQL estar pronto
        wait_for_postgres(engine)
        
        # Limpar tabelas
        truncate_tables(engine)
        
        # ETL: Carregar dados do Data Lake
        etl_movies(engine)
        etl_users(engine)
        etl_ratings(engine)
        
        # Criar views do Data Mart
        create_data_mart_views(engine)
        
        # Executar consultas analiticas
        run_analytics(engine)
        
        print("\n[SUCESSO] ETL concluido com sucesso!")
        
    except Exception as e:
        print(f"\n[ERRO] Erro no ETL: {e}")
        import traceback
        traceback.print_exc()
        exit(1)

if __name__ == "__main__":
    main()
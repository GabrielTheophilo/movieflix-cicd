-- Carregar dados do CSV para as tabelas
COPY movies (id,title,genre,year)
FROM '/data-lake/filmes.csv'
DELIMITER ',' CSV HEADER;

COPY users (id,name,age,country)
FROM '/data-lake/users.csv'
DELIMITER ',' CSV HEADER;

COPY ratings (id,user_id,movie_id,score)
FROM '/data-lake/ratings.csv'
DELIMITER ',' CSV HEADER;
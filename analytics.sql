SELECT m.genre, ROUND(AVG(r.score)::numeric, 2) AS media_avaliacao, COUNT(r.id) AS total_avaliacoes
FROM movies m
JOIN ratings r ON m.id = r.movie_id
GROUP BY m.genre
ORDER BY media_avaliacao DESC, total_avaliacoes DESC
LIMIT 1;

SELECT m.title, COUNT(r.id) AS total_avaliacoes
FROM movies m
JOIN ratings r ON m.id = r.movie_id
GROUP BY m.title
ORDER BY total_avaliacoes DESC
LIMIT 5;

SELECT u.country, COUNT(r.id) AS total_avaliacoes
FROM ratings r
JOIN users u ON r.user_id = u.id
GROUP BY u.country
ORDER BY total_avaliacoes DESC
LIMIT 1;
-- Data Warehouse - MovieFlix
CREATE TABLE movies (
    id INT PRIMARY KEY,
    title TEXT NOT NULL,
    genre TEXT,
    year INT
);

CREATE TABLE users (
    id INT PRIMARY KEY,
    name TEXT NOT NULL,
    age INT,
    country TEXT
);

CREATE TABLE ratings (
    id INT PRIMARY KEY,
    user_id INT REFERENCES users(id),
    movie_id INT REFERENCES movies(id),
    score INT CHECK(score >= 1 AND score <= 5)
);
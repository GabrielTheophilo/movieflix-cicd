import express from 'express';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3000;

const DATA_DIR = '/data-lake';
const MOVIES_FILE = path.join(DATA_DIR, 'filmes.csv');
const RATINGS_FILE = path.join(DATA_DIR, 'ratings.csv');
const USERS_FILE = path.join(DATA_DIR, 'users.csv');

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// ---------------------------------------------------------
// Funções auxiliares CSV
// ---------------------------------------------------------
function ensureDataFiles() {

  if (!fs.existsSync(DATA_DIR)) {
    throw new Error(`${DATA_DIR} não existe ou não está montado corretamente`);
  }

  if (!fs.existsSync(MOVIES_FILE)) {
    fs.writeFileSync(MOVIES_FILE, 'id,title,genre,year\n', 'utf-8');
  }
  if (!fs.existsSync(RATINGS_FILE)) {
    fs.writeFileSync(RATINGS_FILE, 'id,user_id,movie_id,score,comment\n', 'utf-8');
  }
  if (!fs.existsSync(USERS_FILE)) {
    fs.writeFileSync(USERS_FILE, 'id,name,age,country\n', 'utf-8');
  }
}

function parseCSV(content) {
  const lines = content.trim().split('\n');
  if (lines.length <= 1) return [];
  const headers = lines[0].split(',');
  return lines.slice(1).filter(line => line.trim() !== '').map(line => {
    const values = line.split(',');
    const obj = {};
    headers.forEach((h, i) => (obj[h] = values[i] ? values[i].trim() : ''));
    return obj;
  });
}

function readCSV(file) {
  if (!fs.existsSync(file)) return [];
  const content = fs.readFileSync(file, 'utf-8').trim();
  return content ? parseCSV(content) : [];
}

function appendCSV(file, row) {
  fs.appendFileSync(file, row + '\n', 'utf-8');
}

function writeCSV(file, headers, data) {
  const content =
    headers.join(',') +
    '\n' +
    data
      .map(obj => headers.map(h => (obj[h] != null ? obj[h] : '')).join(','))
      .join('\n') +
    '\n';
  fs.writeFileSync(file, content, 'utf-8');
}

function nextId(file) {
  const rows = readCSV(file);
  if (rows.length === 0) return 1;
  const ids = rows.map(r => Number(r.id));
  return Math.max(...ids) + 1;
}

ensureDataFiles();

// ---------------------------------------------------------
// Rotas
// ---------------------------------------------------------

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', service: 'movieflix-app', port: PORT });
});

// ------------------- FILMES -----------------------------

app.get('/api/movies', (_req, res) => {
  const normalizeKeys = obj =>
    Object.fromEntries(
      Object.entries(obj).map(([k, v]) => [k.trim(), v.trim()])
    );
  
  const movies = readCSV(MOVIES_FILE).map(normalizeKeys);
  const ratings = readCSV(RATINGS_FILE).map(normalizeKeys);
  
  const result = movies.map(m => {
    const r = ratings.filter(rt => String(rt.movie_id) === String(m.id));
  
    const scores = r
    .map(rt => Number(String(rt.score).trim()))
    .filter(s => !isNaN(s));
  
  const avg =
    scores.length > 0
      ? Math.floor(scores.reduce((acc, val) => acc + val, 0) / scores.length)
      : '—';
    return {
      ...m,
      year: m.year || 's/ano', // Exibir 's/ano' quando o ano não estiver disponível
      avgRating: avg,
      ratingsCount: r.length
    };
  });

  res.json(result);
});

app.post('/api/movies', (req, res) => {
  const { title, genre, year } = req.body;
  if (!title || !genre) {
    return res.status(400).json({ error: 'title e genre são obrigatórios' });
  }

  const id = nextId(MOVIES_FILE);
  const row = `${id},${escapeCsv(title)},${escapeCsv(genre)},${year ? year : ''}`;
  appendCSV(MOVIES_FILE, row);

  res.status(201).json({ id, title, genre, year });
});

function escapeCsv(val) {
  const s = String(val);
  return s.includes(',') ? `"${s.replace(/"/g, '""')}"` : s;
}

// ------------------- RATINGS -----------------------------

app.post('/api/ratings', (req, res) => {
  const { movieId, user, score} = req.body;
  if (!movieId || score == null) {
    return res.status(400).json({ error: 'movieId e score são obrigatórios' });
  }

  const nScore = Number(score);
  if (Number.isNaN(nScore) || nScore < 1 || nScore > 5) {
    return res.status(400).json({ error: 'score deve ser entre 1 e 5' });
  }

  const movies = readCSV(MOVIES_FILE);
  const movie = movies.find(m => m.id === String(movieId));
  if (!movie) {
    return res.status(404).json({ error: 'Filme não encontrado' });
  }

  // Para simplificar: se user for string, cria novo user se não existir
  let userId = null;
  if (user) {
    const users = readCSV(USERS_FILE);
    let existing = users.find(u => u.name === user);
    if (!existing) {
      const newId = nextId(USERS_FILE);
      appendCSV(USERS_FILE, `${newId},${escapeCsv(user)},,`);
      userId = newId;
    } else {
      userId = existing.id;
    }
  } else {
    userId = '0'; // usuário anônimo
  }

  const ratingId = nextId(RATINGS_FILE);
  const row = `${ratingId},${userId},${movieId},${nScore}`;
  appendCSV(RATINGS_FILE, row);

  res.status(201).json({ id: ratingId, user_id: userId, movie_id: movieId, score: nScore});
});

// ------------------- USERS -----------------------------

app.get('/api/users', (_req, res) => {
  const users = readCSV(USERS_FILE);
  res.json(users);
});

app.post('/api/users', (req, res) => {
  const { id, name, age, country } = req.body;
  if (!name) {
    return res.status(400).json({ error: 'name é obrigatório' });
  }

  // ensure users file read and check for duplicate id when provided
  const users = readCSV(USERS_FILE);

  let userId = id ? String(id) : String(nextId(USERS_FILE));

  // if provided id already exists, return conflict
  if (id) {
    const exists = users.find(u => String(u.id) === String(id));
    if (exists) {
      return res.status(409).json({ error: 'id já existe' });
    }
  }

  const row = `${userId},${escapeCsv(name)},${age != null ? age : ''},${country ? escapeCsv(country) : ''}`;
  appendCSV(USERS_FILE, row);

  res.status(201).json({ id: userId, name, age: age != null ? age : '', country: country || '' });
});

// ---------------------------------------------------------

app.use(express.static(path.join(__dirname, 'public')));

app.get('/', (_req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(PORT, () => {
  console.log(`MovieFlix app rodando em http://0.0.0.0:${PORT}`);
});

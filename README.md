# Server (Express + CORS + PostgreSQL)

Quick scaffold for an Express server using CORS and PostgreSQL.

Setup

1. Copy `.env.example` to `.env` and edit values.

2. Install dependencies:

```powershell
cd server
npm install express cors pg dotenv
npm install --save-dev nodemon
```

Run

Start in production mode:

```powershell
npm run start
```

Start in development mode (auto-restarts):

```powershell
npm run dev
```

Test routes

- `GET /` — healthcheck
- `GET /db` — runs `SELECT NOW()` against PostgreSQL
- `POST /echo` — echoes JSON body

check any error like if smartphone category is render in other category ? and whlie loading any problem

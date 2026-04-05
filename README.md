# Groq All-13 Deploy

This is a clean runtime folder for universal pattern search across 13 symbols.

## Entry Point

- `groqpattern-all13.js`
- npm start command runs:
  1. `prepare-duckdb.js` (ensures `data/backtesting.duckdb` exists)
  2. `groqpattern-all13.js`

## Setup

1. Copy `.env.example` to `.env` and fill values.
2. Install dependencies:

```bash
npm install
```

3. Run:

```bash
npm start
```

## DuckDB In Hosting

Preferred production flow:

1. Upload `backtesting.duckdb` to object storage (S3, R2, GCS, etc).
2. Set `DUCKDB_URL` in `.env`.
3. On startup, `prepare-duckdb.js` downloads the DB into `data/backtesting.duckdb`.

This keeps repo small and works consistently across hosted environments.

## Outputs

- Per-run files: `groq-all13-results/`
- Summary file: `groq-all13-summary.json`
- MongoDB summary write is enabled when `MONGO_URI` is set.


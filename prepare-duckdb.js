"use strict";

const fs = require("fs");
const path = require("path");
const http = require("http");
const https = require("https");

const DATA_DIR = path.join(__dirname, "data");
const DB_FILE = path.join(DATA_DIR, "backtesting.duckdb");
const DUCKDB_URL = String(process.env.DUCKDB_URL || "").trim();

function downloadFile(url, dest) {
  return new Promise((resolve, reject) => {
    const client = url.startsWith("https://") ? https : http;
    const req = client.get(url, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        res.resume();
        return resolve(downloadFile(res.headers.location, dest));
      }
      if (res.statusCode !== 200) {
        res.resume();
        return reject(new Error(`Download failed: HTTP ${res.statusCode}`));
      }
      const tmp = `${dest}.tmp`;
      const out = fs.createWriteStream(tmp);
      res.pipe(out);
      out.on("finish", () => {
        out.close(() => {
          fs.renameSync(tmp, dest);
          resolve();
        });
      });
      out.on("error", reject);
    });
    req.on("error", reject);
  });
}

async function run() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

  if (fs.existsSync(DB_FILE)) {
    const stat = fs.statSync(DB_FILE);
    console.log(`[duckdb] Using existing local DB: ${DB_FILE} (${stat.size} bytes)`);
    return;
  }

  if (!DUCKDB_URL) {
    throw new Error("No local DuckDB found and DUCKDB_URL is not set.");
  }

  console.log(`[duckdb] Downloading database from DUCKDB_URL...`);
  await downloadFile(DUCKDB_URL, DB_FILE);
  const stat = fs.statSync(DB_FILE);
  console.log(`[duckdb] Download complete: ${DB_FILE} (${stat.size} bytes)`);
}

run().catch((err) => {
  console.error("[duckdb] Prepare failed:", err.message || err);
  process.exitCode = 1;
});


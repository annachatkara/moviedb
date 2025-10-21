// server.js
import express from "express";
import bodyParser from "body-parser";
import { createClient } from "@supabase/supabase-js";
import 'dotenv/config';
import cors from 'cors'; // <-- ADD THIS
import jwt from 'jsonwebtoken';
import { Readable } from 'stream';
import readline from 'readline';

const app = express();

// Enable CORS for all routes
app.use(cors()); // <-- ADD THIS

// For parsing JSON
app.use(bodyParser.json({ limit: "50mb" }));

// ENV Vars (set in Vercel)
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.movidbSUPABASE_ANON_KEY
);
console.log("Supabase client initialized:", !!supabase);

// Helpers
function chunkArray(array, size) {
  const result = [];
  for (let i = 0; i < array.length; i += size) {
    result.push(array.slice(i, i + size));
  }
  return result;
}

function authMiddleware(req, res, next) {
  const authHeader = req.headers['authorization'];
  if (!authHeader) return res.status(401).json({ error: "Unauthorized" });

  const token = authHeader.split(' ')[1];
  jwt.verify(token, process.env.MY_API_KEY, (err, decoded) => {
    if (err) return res.status(403).json({ error: "Forbidden" });
    req.user = decoded;
    next();
  });
}

// Insert helper with chunking
async function insertChunks(tableName, rows, chunkSize = 30, mode = 'skip') {
  let insertedTotal = 0;
  const chunks = chunkArray(rows, chunkSize);
  for (const [i, chunk] of chunks.entries()) {
    let q = supabase.from(tableName);
    if (mode === 'skip') {
      const { error } = await q.upsert(chunk, { onConflict: 'id', ignoreDuplicates: true });
      if (error) return { error: `Chunk ${i+1}/${chunks.length} failed: ${error.message}`, insertedTotal };
    } else if (mode === 'merge') {
      const { error } = await q.upsert(chunk, { onConflict: 'id' });
      if (error) return { error: `Chunk ${i+1}/${chunks.length} failed: ${error.message}`, insertedTotal };
    } else {
      const { error } = await q.insert(chunk);
      if (error) return { error: `Chunk ${i+1}/${chunks.length} failed: ${error.message}`, insertedTotal };
    }
    insertedTotal += chunk.length;
  }
  return { insertedTotal };
}

// Stream NDJSON from remote URL and insert in chunks
async function ingestNdjsonFromUrl(tableName, fileUrl, chunkSize = 30) {
  const resp = await fetch(fileUrl);
  if (!resp.ok) {
    throw new Error(`Failed to fetch fileUrl: ${resp.status} ${resp.statusText}`);
  }

  let nodeStream;
  if (resp.body && typeof Readable.fromWeb === 'function' && resp.body.getReader) {
    nodeStream = Readable.fromWeb(resp.body);
  } else {
    nodeStream = resp.body;
  }

  const rl = readline.createInterface({ input: nodeStream, crlfDelay: Infinity });
  let buffer = [];
  let total = 0;

  for await (const line of rl) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    const obj = JSON.parse(trimmed);
    buffer.push(obj);
    if (buffer.length >= chunkSize) {
      const { error, insertedTotal } = await insertChunks(tableName, buffer, chunkSize);
      if (error) throw new Error(error);
      total += insertedTotal;
      buffer = [];
    }
  }
  if (buffer.length) {
    const { error, insertedTotal } = await insertChunks(tableName, buffer, chunkSize);
    if (error) throw new Error(error);
    total += insertedTotal;
  }
  return total;
}

// -------- CRUD Routes Generator --------
function generateRoutes(tableName) {
  // (All your route definitions remain unchanged here... just keep them as they were.)
  // CREATE, READ, SEARCH, UPDATE, DELETE, etc.
  // You can keep everything here as-is.
  // ...
}

// Extra route
app.post(`/api/request`, async (req, res) => {
  try {
    const { type, type_id, name } = req.body;
    if (!type || !type_id || !name) {
      return res.status(400).json({ error: "type, type_id and name are required" });
    }

    const { data, error } = await supabase
      .from("requests") 
      .insert([{ type, type_id, name }]);

    if (error) return res.status(400).json({ error: error.message });

    return res.status(201).json({ message: "Data inserted successfully", data });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Initialize routes for each table
["movies", "series", "anime", "trendingmovies", "hlstoken"].forEach(generateRoutes);

// Health check
app.get("/", (_req, res) => {
  res.json({ ok: true });
});

export default app;

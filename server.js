// server.js
import express from "express";
import bodyParser from "body-parser";
import { createClient } from "@supabase/supabase-js";
import 'dotenv/config';
import jwt from 'jsonwebtoken';
import { Readable } from 'stream';
import readline from 'readline';

const app = express();
app.use(bodyParser.json({ limit: "50mb" })); // client-side limit

// ENV Vars (set in Vercel)
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_ANON_KEY
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

  // Convert Web ReadableStream -> Node Readable for readline
  let nodeStream;
  if (resp.body && typeof Readable.fromWeb === 'function' && resp.body.getReader) {
    nodeStream = Readable.fromWeb(resp.body);
  } else {
    // Fallback if already Node stream
    nodeStream = resp.body;
  }

  const rl = readline.createInterface({ input: nodeStream, crlfDelay: Infinity });
  let buffer = [];
  let total = 0;

  for await (const line of rl) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    // Expect each line to be a JSON object (NDJSON)
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
  // CREATE (single / bulk with chunking=5000 for normal bulk)
  app.post(`/api/${tableName}`, authMiddleware, async (req, res) => {
    try {
      const data = req.body;
      if (Array.isArray(data)) {
        // bulk insert (normal)
        const chunks = chunkArray(data, 5000);
        for (const chunk of chunks) {
          const { error } = await supabase.from(tableName).insert(chunk);
          if (error) return res.status(400).json({ error: error.message });
        }
        return res.json({ message: `${data.length} records inserted into ${tableName}` });
      } else {
        // single insert
        const { data: inserted, error } = await supabase.from(tableName).insert([data]).select();
        if (error) return res.status(400).json({ error: error.message });
        return res.json(inserted[0]);
      }
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // READ with pagination
  app.get(`/api/${tableName}`, async (req, res) => {
    try {
      const page = parseInt(req.query.page) || 1;
      const limit = 30;
      const from = (page - 1) * limit;
      const to = from + limit - 1;

      const { data, error, count } = await supabase
        .from(tableName)
        .select("*", { count: "exact" })
        .order("release_date", { ascending: false })
        .range(from, to);

      if (error) return res.status(400).json({ error: error.message });

      return res.json({
        page,
        total: count,
        totalPages: Math.ceil(count / limit),
        results: data,
      });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // Specific routes FIRST to avoid collision with :id
  // READ only rows with video_url present, with pagination
  app.get(`/api/${tableName}/with-video`, async (req, res) => {
    try {
      const page = parseInt(req.query.page) || 1;
      const limit = 30;
      const from = (page - 1) * limit;
      const to = from + limit - 1;

      const { data, error, count } = await supabase
        .from(tableName)
        .select("*", { count: "exact" })
        .not("video_url", "is", null)
        .neq("video_url", "")
        .order("release_date", { ascending: false })
        .range(from, to);

      if (error) return res.status(400).json({ error: error.message });

      return res.json({
        page,
        total: count,
        totalPages: Math.ceil(count / limit),
        results: data,
      });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // LIVE SEARCH: by original_title (ilike) or direct by id
  // GET /api/{table}/search?q=Avenger&page=1&count=estimated
  // GET /api/{table}/search?id=123
  app.get(`/api/${tableName}/search`, async (req, res) => {
    try {
      const q = (req.query.q || '').trim();
      const id = (req.query.id || '').trim();

      // If id is provided, return direct lookup (single)
      if (id) {
        const { data, error } = await supabase
          .from(tableName)
          .select("*")
          .eq("id", id)
          .single();
        if (error) return res.status(404).json({ error: error.message });
        return res.json(data);
      }

      if (!q) {
        return res.status(400).json({ error: "Provide either ?q or ?id" });
      }

      const page = parseInt(req.query.page) || 1;
      const limit = 30;
      const from = (page - 1) * limit;
      const to = from + limit - 1;
      const countMode = req.query.count === 'exact' ? 'exact' : 'estimated';

      const { data, error, count } = await supabase
        .from(tableName)
        .select("*", { count: countMode })
        .ilike("original_title", `%${q}%`)
        .order("release_date", { ascending: false })
        .range(from, to);

      if (error) return res.status(400).json({ error: error.message });

      return res.json({
        page,
        total: count ?? null,
        totalPages: count != null ? Math.ceil(count / limit) : null,
        results: data,
      });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // Parameterized id route LAST, with numeric guard (bigint)
  app.get(`/api/${tableName}/:id(\\d+)`, async (req, res) => {
    try {
      const { data, error } = await supabase
        .from(tableName)
        .select("*")
        .eq("id", Number(req.params.id))
        .single();
      if (error) return res.status(404).json({ error: error.message });
      return res.json(data);
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // UPDATE
  app.put(`/api/${tableName}/:id`, authMiddleware, async (req, res) => {
    try {
      const { data, error } = await supabase.from(tableName).update(req.body).eq("id", req.params.id).select();
      if (error) return res.status(400).json({ error: error.message });
      return res.json(data[0]);
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // DELETE
  app.delete(`/api/${tableName}/:id`, authMiddleware, async (req, res) => {
    try {
      const { error } = await supabase.from(tableName).delete().eq("id", req.params.id);
      if (error) return res.status(400).json({ error: error.message });
      return res.json({ message: `${tableName} with id ${req.params.id} deleted` });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // NEW: BULK UPLOAD endpoint (30-row chunks by default)
  // Accepts either:
  //  - JSON array via { items: [...] }
  //  - NDJSON via { fileUrl: "https://..." } where each line is a JSON object
  // Optional override: ?chunkSize=30
  app.post(`/api/${tableName}/bulk-upload`, authMiddleware, async (req, res) => {
    try {
      const chunkSize = Math.max(1, parseInt(req.query.chunkSize) || 30);
      if (Array.isArray(req.body)) {
        const { error, insertedTotal } = await insertChunks(tableName, req.body, chunkSize);
        if (error) return res.status(400).json({ error });
        return res.json({ message: `Inserted ${insertedTotal} records into ${tableName} in chunks of ${chunkSize}` });
      }

      const { items, fileUrl } = req.body || {};
      if (Array.isArray(items)) {
        const { error, insertedTotal } = await insertChunks(tableName, items, chunkSize);
        if (error) return res.status(400).json({ error });
        return res.json({ message: `Inserted ${insertedTotal} records into ${tableName} in chunks of ${chunkSize}` });
      }

      if (fileUrl) {
        const total = await ingestNdjsonFromUrl(tableName, fileUrl, chunkSize);
        return res.json({ message: `Stream-inserted ${total} records into ${tableName} in chunks of ${chunkSize}` });
      }

      return res.status(400).json({ error: "Provide either an array body, { items: [...] }, or { fileUrl } pointing to NDJSON" });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });
}

// Tables: movies, series, anime
["movies", "series", "anime"].forEach(generateRoutes);

// Health
app.get("/", (_req, res) => {
  res.json({ ok: true });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});

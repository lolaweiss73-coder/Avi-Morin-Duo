import express from "express";
import pg from "pg";
import crypto from "crypto";
import OpenAI from "openai";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const { Pool } = pg;
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
const PORT = Number(process.env.PORT || 3000);

const OPENAI_MODEL = process.env.OPENAI_MODEL || "gpt-5.6-sol";
const OPENROUTER_AVI_MODEL = process.env.OPENROUTER_AVI_MODEL || "openai/gpt-5.6";
const OPENROUTER_MORIN_MODEL = process.env.OPENROUTER_MORIN_MODEL || "openai/gpt-5.6";
const OPENROUTER_RESEARCH_MODEL = process.env.OPENROUTER_RESEARCH_MODEL || OPENROUTER_AVI_MODEL;
const PROMPT_DIR = process.env.PROMPT_DIR || "/data/prompts";
const PROMPT_SEED_DIR = path.join(__dirname, "prompts");
const PROMPT_FILES = new Set([
  "mission.txt",
  "avi-digital.txt",
  "morin.txt",
  "validation-system.txt",
  "validation-user.txt",
  "summarizer.txt",
  "reward-avi.txt",
  "reward-morin.txt",
  "reward-intro.txt",
  "reward-breakthrough.txt",
  "reward-continue.txt",
  "mission-wrapper.txt",
  "summary-wrapper.txt",
  "research-first-turn.txt",
  "research-continue.txt",
  "fantasy-avi.txt",
  "fantasy-morin.txt",
  "fantasy-first-turn.txt",
  "fantasy-continue.txt"
]);

function promptPath(name) {
  if (!PROMPT_FILES.has(name)) throw new Error("Unknown prompt file");
  return path.join(PROMPT_DIR, name);
}

function seedPromptPath(name) {
  if (!PROMPT_FILES.has(name)) throw new Error("Unknown prompt file");
  return path.join(PROMPT_SEED_DIR, name);
}

function initPromptStore() {
  fs.mkdirSync(PROMPT_DIR, { recursive: true });
  for (const name of PROMPT_FILES) {
    const dest = promptPath(name);
    if (!fs.existsSync(dest)) {
      fs.copyFileSync(seedPromptPath(name), dest);
    }
  }
}

function loadPrompt(name, vars = {}) {
  const live = promptPath(name);
  const source = fs.existsSync(live) ? live : seedPromptPath(name);
  let text = fs.readFileSync(source, "utf8").trim();
  for (const [key, value] of Object.entries(vars)) {
    text = text.split(`{{${key}}}`).join(String(value ?? ""));
  }
  return text;
}

const REWARD_SECONDS = Number(process.env.REWARD_SECONDS || 120);
const APP_PIN = process.env.APP_PIN || "";
const DATABASE_URL = process.env.DATABASE_URL || "";
const OPENROUTER_API_KEY = process.env.OPENROUTER_API_KEY || process.env.OPEN_ROUTER_API_KEY || "";

app.use(express.json({ limit: "1mb" }));
app.use(express.static(path.join(__dirname, "public")));

const pool = DATABASE_URL
  ? new Pool({ connectionString: DATABASE_URL, ssl: DATABASE_URL.includes("localhost") ? false : { rejectUnauthorized: false } })
  : null;

const memory = {
  runs: new Map(),
  events: new Map(),
};

const activeRuns = new Set();
const sseClients = new Map();

function nowIso() {
  return new Date().toISOString();
}
function wallTimeNs() {
  return (BigInt(Date.now()) * 1000000n).toString();
}
function monotonicNs() {
  return process.hrtime.bigint().toString();
}
function safeJson(value) {
  try { return JSON.parse(JSON.stringify(value)); } catch { return { value: String(value) }; }
}

async function initDb() {
  if (!pool) return;
  await pool.query(`
    CREATE TABLE IF NOT EXISTS runs (
      id text PRIMARY KEY,
      created_at timestamptz NOT NULL DEFAULT now(),
      updated_at timestamptz NOT NULL DEFAULT now(),
      status text NOT NULL,
      phase text NOT NULL DEFAULT 'research',
      topic text,
      mission text NOT NULL,
      duration_minutes integer NOT NULL,
      deadline_at timestamptz NOT NULL,
      current_round integer NOT NULL DEFAULT 0,
      stop_requested boolean NOT NULL DEFAULT false,
      research_model text NOT NULL,
      reward_model_avi text,
      reward_model_morin text,
      summary text,
      error text
    );
    CREATE TABLE IF NOT EXISTS run_events (
      id bigserial PRIMARY KEY,
      run_id text NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
      seq bigint NOT NULL,
      created_at timestamptz NOT NULL DEFAULT now(),
      phase text NOT NULL,
      round integer NOT NULL DEFAULT 0,
      speaker text NOT NULL,
      event_type text NOT NULL,
      model text,
      latency_ms integer,
      wall_time_ns text,
      monotonic_ns text,
      payload jsonb NOT NULL DEFAULT '{}'::jsonb,
      text_content text,
      UNIQUE(run_id, seq)
    );
    ALTER TABLE runs ADD COLUMN IF NOT EXISTS mode text NOT NULL DEFAULT 'breakthrough';
    CREATE INDEX IF NOT EXISTS run_events_run_seq_idx ON run_events(run_id, seq);
    CREATE INDEX IF NOT EXISTS runs_updated_at_idx ON runs(updated_at DESC);
  `);
}

async function createRun({ topic, durationMinutes, mode = "breakthrough" }) {
  const id = crypto.randomUUID();
  const createdAt = nowIso();
  const deadlineAt = new Date(Date.now() + durationMinutes * 60_000).toISOString();
  const run = {
    id,
    createdAt,
    updatedAt: createdAt,
    status: "queued",
    mode,
    phase: mode === "fantasy" ? "fantasy" : "research",
    topic: topic || null,
    mission: topic?.trim() || loadPrompt("mission.txt"),
    durationMinutes,
    deadlineAt,
    currentRound: 0,
    stopRequested: false,
    researchModel: OPENAI_MODEL,
    rewardModelAvi: OPENROUTER_AVI_MODEL,
    rewardModelMorin: OPENROUTER_MORIN_MODEL,
    summary: null,
    error: null,
  };
  if (pool) {
    await pool.query(
      `INSERT INTO runs
       (id,status,mode,phase,topic,mission,duration_minutes,deadline_at,current_round,stop_requested,research_model,reward_model_avi,reward_model_morin,summary,error)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`,
      [run.id, run.status, run.mode, run.phase, run.topic, run.mission, run.durationMinutes, run.deadlineAt, 0, false, run.researchModel, run.rewardModelAvi, run.rewardModelMorin, null, null]
    );
  } else {
    memory.runs.set(id, run);
    memory.events.set(id, []);
  }
  return run;
}

function mapDbRun(r) {
  if (!r) return null;
  return {
    id: r.id,
    createdAt: r.created_at,
    updatedAt: r.updated_at,
    status: r.status,
    mode: r.mode || "breakthrough",
    phase: r.phase,
    topic: r.topic,
    mission: r.mission,
    durationMinutes: r.duration_minutes,
    deadlineAt: r.deadline_at,
    currentRound: r.current_round,
    stopRequested: r.stop_requested,
    researchModel: r.research_model,
    rewardModelAvi: r.reward_model_avi,
    rewardModelMorin: r.reward_model_morin,
    summary: r.summary,
    error: r.error,
  };
}

async function getRun(id) {
  if (pool) {
    const { rows } = await pool.query("SELECT * FROM runs WHERE id=$1", [id]);
    return mapDbRun(rows[0]);
  }
  return memory.runs.get(id) || null;
}

async function updateRun(id, patch) {
  const run = await getRun(id);
  if (!run) return null;
  const next = { ...run, ...patch, updatedAt: nowIso() };
  if (pool) {
    await pool.query(
      `UPDATE runs SET
        updated_at=now(), status=$2, mode=$3, phase=$4, topic=$5, mission=$6, duration_minutes=$7,
        deadline_at=$8, current_round=$9, stop_requested=$10, research_model=$11,
        reward_model_avi=$12, reward_model_morin=$13, summary=$14, error=$15
       WHERE id=$1`,
      [id, next.status, next.mode || "breakthrough", next.phase, next.topic, next.mission, next.durationMinutes, next.deadlineAt,
       next.currentRound, next.stopRequested, next.researchModel, next.rewardModelAvi,
       next.rewardModelMorin, next.summary, next.error]
    );
  } else {
    memory.runs.set(id, next);
  }
  return next;
}

async function nextSeq(runId) {
  if (pool) {
    const { rows } = await pool.query("SELECT COALESCE(MAX(seq),0)::bigint + 1 AS seq FROM run_events WHERE run_id=$1", [runId]);
    return Number(rows[0].seq);
  }
  return (memory.events.get(runId)?.length || 0) + 1;
}

async function addEvent(runId, event) {
  const seq = await nextSeq(runId);
  const row = {
    seq,
    runId,
    createdAt: nowIso(),
    phase: event.phase || "research",
    round: event.round || 0,
    speaker: event.speaker || "system",
    eventType: event.eventType || "event",
    model: event.model || null,
    latencyMs: event.latencyMs ?? null,
    wallTimeNs: wallTimeNs(),
    monotonicNs: monotonicNs(),
    payload: safeJson(event.payload || {}),
    textContent: event.textContent ?? null,
  };
  if (pool) {
    await pool.query(
      `INSERT INTO run_events
       (run_id,seq,phase,round,speaker,event_type,model,latency_ms,wall_time_ns,monotonic_ns,payload,text_content)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
      [runId,row.seq,row.phase,row.round,row.speaker,row.eventType,row.model,row.latencyMs,row.wallTimeNs,row.monotonicNs,row.payload,row.textContent]
    );
  } else {
    const list = memory.events.get(runId) || [];
    list.push(row);
    memory.events.set(runId, list);
  }
  broadcast(runId, row);
  return row;
}

async function listEvents(runId, limit = 1000) {
  if (pool) {
    const { rows } = await pool.query(
      "SELECT * FROM run_events WHERE run_id=$1 ORDER BY seq ASC LIMIT $2",
      [runId, limit]
    );
    return rows.map(r => ({
      seq: Number(r.seq), runId: r.run_id, createdAt: r.created_at, phase: r.phase, round: r.round,
      speaker: r.speaker, eventType: r.event_type, model: r.model, latencyMs: r.latency_ms,
      wallTimeNs: r.wall_time_ns, monotonicNs: r.monotonic_ns, payload: r.payload, textContent: r.text_content
    }));
  }
  return (memory.events.get(runId) || []).slice(0, limit);
}

function broadcast(runId, event) {
  const clients = sseClients.get(runId);
  if (!clients) return;
  const payload = `data: ${JSON.stringify(event)}\n\n`;
  for (const res of clients) {
    try { res.write(payload); } catch {}
  }
}

function auth(req, res, next) {
  if (!APP_PIN) return next();
  const pin = req.get("x-app-pin") || req.query.pin;
  if (pin !== APP_PIN) return res.status(401).json({ error: "PIN required" });
  next();
}

app.get("/health", async (_req, res) => {
  let db = "memory";
  try {
    if (pool) {
      await pool.query("SELECT 1");
      db = "postgres";
    }
    res.json({ ok: true, db, activeRuns: activeRuns.size, time: nowIso() });
  } catch (error) {
    res.status(503).json({ ok: false, error: String(error) });
  }
});

app.use("/api", auth);

app.get("/api/config", (_req, res) => {
  res.json({
    openaiModel: OPENAI_MODEL,
    rewardModelAvi: OPENROUTER_AVI_MODEL,
    rewardModelMorin: OPENROUTER_MORIN_MODEL,
    openRouterResearchModel: OPENROUTER_RESEARCH_MODEL,
    hasOpenAI: Boolean(process.env.OPENAI_API_KEY),
    hasOpenRouter: Boolean(OPENROUTER_API_KEY),
    hasDatabase: Boolean(DATABASE_URL),
    pinRequired: Boolean(APP_PIN),
    rewardSeconds: REWARD_SECONDS,
    promptDir: PROMPT_DIR,
    promptFiles: [...PROMPT_FILES].sort(),
  });
});

app.post("/api/runs", async (req, res) => {
  try {
    const durationMinutes = Number(req.body?.durationMinutes || 10);
    if (!Number.isFinite(durationMinutes) || durationMinutes < 1 || durationMinutes > 600) {
      return res.status(400).json({ error: "durationMinutes must be between 1 and 600" });
    }
    if (!process.env.OPENAI_API_KEY && !OPENROUTER_API_KEY) return res.status(503).json({ error: "No model provider API key is configured" });
    const mode = String(req.body?.mode || "breakthrough");
    if (!["breakthrough","fantasy"].includes(mode)) return res.status(400).json({ error: "mode must be breakthrough or fantasy" });
    if (mode === "fantasy" && !OPENROUTER_API_KEY) return res.status(503).json({ error: "Fantasy mode requires OpenRouter" });
    const run = await createRun({ topic: String(req.body?.topic || "").trim(), durationMinutes, mode });
    res.status(202).json(run);
    setImmediate(() => runLoop(run.id).catch(err => console.error("runLoop", run.id, err)));
  } catch (error) {
    res.status(500).json({ error: String(error) });
  }
});

app.get("/api/prompts", (_req, res) => {
  try {
    const prompts = [...PROMPT_FILES].sort().map(name => ({
      name,
      content: loadPrompt(name)
    }));
    res.json({ promptDir: PROMPT_DIR, prompts });
  } catch (error) {
    res.status(500).json({ error: String(error) });
  }
});

app.get("/api/prompts/:name", (req, res) => {
  try {
    const name = String(req.params.name || "");
    res.type("text/plain; charset=utf-8").send(loadPrompt(name));
  } catch (error) {
    res.status(404).json({ error: String(error) });
  }
});

app.put("/api/prompts/:name", (req, res) => {
  try {
    const name = String(req.params.name || "");
    if (!PROMPT_FILES.has(name)) return res.status(404).json({ error: "Unknown prompt file" });
    const content = typeof req.body?.content === "string" ? req.body.content : "";
    if (!content.trim()) return res.status(400).json({ error: "Prompt content cannot be empty" });
    fs.mkdirSync(PROMPT_DIR, { recursive: true });
    fs.writeFileSync(promptPath(name), content, "utf8");
    res.json({ ok: true, name, bytes: Buffer.byteLength(content, "utf8") });
  } catch (error) {
    res.status(500).json({ error: String(error) });
  }
});

app.get("/api/runs", async (_req, res) => {
  try {
    if (pool) {
      const { rows } = await pool.query("SELECT * FROM runs ORDER BY created_at DESC LIMIT 200");
      return res.json(rows.map(mapDbRun));
    }
    res.json([...memory.runs.values()].sort((a,b) => String(b.createdAt).localeCompare(String(a.createdAt))).slice(0,200));
  } catch (error) {
    res.status(500).json({ error: String(error) });
  }
});

app.get("/api/runs/:id/finals", async (req, res) => {
  const run = await getRun(req.params.id);
  if (!run) return res.status(404).json({ error: "Run not found" });
  const events = await listEvents(run.id, 100000);
  res.json(events
    .filter(e => (e.phase === "research" && e.eventType === "final") || e.eventType === "fantasy_final" || e.eventType === "reward_final")
    .map(e => ({
      phase: e.phase,
      eventType: e.eventType,
      seq: e.seq,
      round: e.round,
      speaker: e.speaker,
      model: e.model,
      latencyMs: e.latencyMs,
      textContent: e.textContent
    })));
});

app.get("/api/runs/:id", async (req, res) => {
  const run = await getRun(req.params.id);
  if (!run) return res.status(404).json({ error: "Run not found" });
  res.json(run);
});

app.post("/api/runs/:id/stop", async (req, res) => {
  const run = await getRun(req.params.id);
  if (!run) return res.status(404).json({ error: "Run not found" });
  await updateRun(run.id, { stopRequested: true, status: "stopping" });
  await addEvent(run.id, { phase: run.phase, round: run.currentRound, speaker: "system", eventType: "stop_requested", textContent: "STOP requested by user." });
  res.json({ ok: true });
});

app.get("/api/runs/:id/events", async (req, res) => {
  const run = await getRun(req.params.id);
  if (!run) return res.status(404).end();
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache, no-transform");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders?.();

  const prior = await listEvents(run.id, 5000);
  for (const ev of prior) res.write(`data: ${JSON.stringify(ev)}\n\n`);

  const clients = sseClients.get(run.id) || new Set();
  clients.add(res);
  sseClients.set(run.id, clients);
  const keepAlive = setInterval(() => { try { res.write(": ping\n\n"); } catch {} }, 20000);
  req.on("close", () => {
    clearInterval(keepAlive);
    clients.delete(res);
    if (!clients.size) sseClients.delete(run.id);
  });
});

app.get("/api/runs/:id/export.jsonl", async (req, res) => {
  const run = await getRun(req.params.id);
  if (!run) return res.status(404).json({ error: "Run not found" });
  const events = await listEvents(run.id, 100000);
  res.setHeader("Content-Type", "application/x-ndjson; charset=utf-8");
  res.setHeader("Content-Disposition", `attachment; filename="avi-morin-${run.id}.jsonl"`);
  res.write(JSON.stringify({ type: "run", run }) + "\n");
  for (const event of events) res.write(JSON.stringify({ type: "event", ...event }) + "\n");
  res.end();
});

app.get("/api/runs/:id/export.txt", async (req, res) => {
  try {
    const run = await getRun(req.params.id);
    if (!run) return res.status(404).json({ error: "Run not found" });
    const events = await listEvents(run.id, 100000);
    const dtf = new Intl.DateTimeFormat("he-IL", {
      timeZone: "Asia/Jerusalem", year: "numeric", month: "2-digit",
      day: "2-digit", hour: "2-digit", minute: "2-digit", hour12: false
    });
    const date = x => x ? dtf.format(new Date(x)) : "—";
    const labels = { breakthrough: "מחקר", fantasy: "משחק תפקידים" };
    const lines = [
      "Avi Morin Duo — תמליל השיחה",
      "מזהה: " + run.id,
      "מצב: " + (labels[run.mode] || run.mode || "מחקר"),
      "נושא: " + (run.topic || run.mission || "ללא נושא"),
      "התחלה: " + date(run.createdAt),
      "סטטוס: " + run.status,
      "========================================"
    ];
    const included = new Set([
      "final", "fantasy_final", "reward_final", "breakthrough_proposed",
      "breakthrough_validated", "breakthrough_validation", "agent_error",
      "run_completed", "run_failed", "provider_terminal_error"
    ]);
    for (const e of events) {
      if (!included.has(e.eventType) || !e.textContent?.trim()) continue;
      lines.push(
        "[" + date(e.createdAt) + "] " + e.speaker + " · סבב " + e.round +
          " · " + e.eventType,
        e.textContent.trim(),
        "----------------------------------------"
      );
    }
    res.type("text/plain; charset=utf-8");
    res.setHeader("Content-Disposition", 'attachment; filename="avi-morin-' + run.id + '.txt"');
    res.send("\uFEFF" + lines.join("\n\n"));
  } catch (error) {
    res.status(500).json({ error: String(error) });
  }
});

function researchPersona(name) {
  return loadPrompt(name === "Avi Digital" ? "avi-digital.txt" : "morin.txt");
}

function extractMeta(text) {
  const match = text.match(/\\[\\[META\\s+({.*})\\]\\]\\s*$/s);
  let meta = { breakthrough: false, proposal: null };
  let visible = text.trim();
  if (match) {
    try { meta = { ...meta, ...JSON.parse(match[1]) }; } catch {}
    visible = text.slice(0, match.index).trim();
  }
  return { visible, meta };
}

async function buildResearchMessages(run, speaker) {
  const events = await listEvents(run.id, 100000);
  const finals = events.filter(e => e.phase === "research" && e.eventType === "final" && (e.speaker === "Avi Digital" || e.speaker === "Morin"));
  const recent = finals.slice(-24);
  const messages = [
    { role: "system", content: researchPersona(speaker) },
    { role: "system", content: loadPrompt("mission-wrapper.txt", { mission: run.mission }) },
  ];
  if (run.summary) messages.push({ role: "system", content: loadPrompt("summary-wrapper.txt", { summary: run.summary }) });
  for (const e of recent) {
    messages.push({
      role: e.speaker === speaker ? "assistant" : "user",
      content: `${e.speaker}: ${e.textContent}`
    });
  }
  if (!recent.length) {
    messages.push({ role: "user", content: loadPrompt("research-first-turn.txt") });
  } else {
    messages.push({ role: "user", content: loadPrompt("research-continue.txt") });
  }
  return messages;
}

async function streamOpenAI({ run, speaker, round, messages, eventTypePrefix = "research" }) {
  const started = Date.now();
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 180000);
  try {
    const stream = await openai.responses.create({
      model: OPENAI_MODEL,
      input: messages,
      reasoning: { effort: "medium" },
      stream: true
    }, { signal: controller.signal });

    let full = "";
    let doneText = "";
    let sawTerminal = false;
    for await (const evt of stream) {
      if (evt.type === "error") {
        const detail = evt.error || evt;
        throw new Error(`OpenAI stream error: ${JSON.stringify(detail)}`);
      }
      if (evt.type === "response.failed") {
        throw new Error(`OpenAI response failed: ${JSON.stringify(evt.response?.error || evt)}`);
      }
      if (evt.type === "response.incomplete") {
        throw new Error(`OpenAI response incomplete: ${JSON.stringify(evt.response?.incomplete_details || evt)}`);
      }
      if (evt.type === "response.completed" || evt.type === "response.output_text.done") {
        sawTerminal = true;
      }
      if (evt.type === "response.output_text.done" && typeof evt.text === "string") {
        doneText = evt.text;
      }
      if (evt.type === "response.output_text.delta" && typeof evt.delta === "string") {
        full += evt.delta;
        await addEvent(run.id, {
          phase: "research",
          round,
          speaker,
          eventType: `${eventTypePrefix}_chunk`,
          model: OPENAI_MODEL,
          payload: { delta: evt.delta, sequenceNumber: evt.sequence_number ?? null },
          textContent: evt.delta
        });
      }
    }

    if (!full && doneText) full = doneText;
    if (!full) throw new Error("OpenAI returned no output text");
    if (!sawTerminal) throw new Error("OpenAI stream ended before a terminal completion event");
    return { text: full.trim(), latencyMs: Date.now() - started };
  } finally {
    clearTimeout(timeout);
  }
}

async function streamOpenRouterResearch({ run, speaker, round, messages, eventTypePrefix = "research" }) {
  if (!OPENROUTER_API_KEY) throw new Error("OPENROUTER_API_KEY is not configured");
  const started = Date.now();
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 180000);
  try {
    const response = await fetch("https://openrouter.ai/api/v1/chat/completions", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${OPENROUTER_API_KEY}`,
        "Content-Type": "application/json",
        "HTTP-Referer": process.env.PUBLIC_URL || "https://railway.app",
        "X-Title": "Avi Morin Duo"
      },
      body: JSON.stringify({
        model: OPENROUTER_RESEARCH_MODEL,
        messages,
        stream: true
      }),
      signal: controller.signal
    });

    if (!response.ok || !response.body) {
      const body = await response.text();
      throw new Error(`OpenRouter ${response.status}: ${body.slice(0, 800)}`);
    }

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    let full = "";
    let sawTerminal = false;

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split("\n");
      buffer = lines.pop() || "";
      for (const line of lines) {
        if (!line.startsWith("data: ")) continue;
        const raw = line.slice(6).trim();
        if (!raw) continue;
        if (raw === "[DONE]") { sawTerminal = true; continue; }
        let evt;
        try { evt = JSON.parse(raw); } catch { continue; }
        if (evt?.error) throw new Error(`OpenRouter stream error: ${JSON.stringify(evt.error)}`);
        if (evt?.choices?.[0]?.finish_reason) sawTerminal = true;
        const delta = evt?.choices?.[0]?.delta?.content;
        if (typeof delta === "string" && delta) {
          full += delta;
          await addEvent(run.id, {
            phase: "research",
            round,
            speaker,
            eventType: `${eventTypePrefix}_chunk`,
            model: OPENROUTER_RESEARCH_MODEL,
            payload: { delta, provider: "openrouter" },
            textContent: delta
          });
        }
      }
    }

    if (!full) throw new Error("OpenRouter returned no output text");
    if (!sawTerminal) throw new Error("OpenRouter research stream ended before a terminal completion signal");
    return {
      text: full.trim(),
      latencyMs: Date.now() - started,
      model: OPENROUTER_RESEARCH_MODEL,
      provider: "openrouter"
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function streamResearchModel(args) {
  if (process.env.OPENAI_API_KEY) {
    try {
      const result = await streamOpenAI(args);
      return { ...result, model: OPENAI_MODEL, provider: "openai" };
    } catch (error) {
      if (!OPENROUTER_API_KEY) throw error;
      await addEvent(args.run.id, {
        phase: "research",
        round: args.round,
        speaker: "system",
        eventType: "research_provider_fallback",
        model: OPENAI_MODEL,
        payload: {
          from: "openai",
          to: "openrouter",
          error: String(error)
        },
        textContent: "OpenAI was unavailable for this turn. Continuing through OpenRouter."
      });
      return streamOpenRouterResearch(args);
    }
  }

  return streamOpenRouterResearch(args);
}

async function validateBreakthrough(run, proposer, proposal, round) {
  const validator = proposer === "Avi Digital" ? "Morin" : "Avi Digital";
  const messages = [
    { role: "system", content: researchPersona(validator) },
    { role: "system", content: loadPrompt("validation-system.txt") },
    { role: "user", content: loadPrompt("validation-user.txt", { proposal }) }
  ];
  const result = await streamResearchModel({ run, speaker: validator, round, messages, eventTypePrefix: "validation" });
  const m = result.text.match(/\\[\\[VALIDATION\\s+({.*})\\]\\]\\s*$/s);
  let verdict = { valid: false, reason: "Could not parse validation" };
  let visible = result.text;
  if (m) {
    try { verdict = { ...verdict, ...JSON.parse(m[1]) }; } catch {}
    visible = result.text.slice(0, m.index).trim();
  }
  await addEvent(run.id, {
    phase: "research", round, speaker: validator, eventType: "breakthrough_validation",
    model: result.model || OPENAI_MODEL, latencyMs: result.latencyMs,
    payload: { proposal, proposer, verdict }, textContent: visible
  });
  return verdict;
}

async function maybeSummarize(run, round) {
  if (round === 0 || round % 12 !== 0) return;
  const events = await listEvents(run.id, 100000);
  const finals = events.filter(e => e.phase === "research" && e.eventType === "final").slice(-30);
  const text = finals.map(e => `${e.speaker}: ${e.textContent}`).join("\n\n");
  if (!text) return;
  const messages = [
    { role: "system", content: loadPrompt("summarizer.txt") },
    { role: "user", content: text }
  ];
  try {
    const result = await streamResearchModel({ run, speaker: "System Summarizer", round, messages, eventTypePrefix: "summary" });
    await updateRun(run.id, { summary: result.text });
    await addEvent(run.id, { phase: "research", round, speaker: "system", eventType: "summary_updated", model: result.model || OPENAI_MODEL, latencyMs: result.latencyMs, textContent: result.text });
  } catch (error) {
    await addEvent(run.id, { phase: "research", round, speaker: "system", eventType: "summary_error", payload: { error: String(error) } });
  }
}

function rewardPersona(name) {
  return loadPrompt(name === "Avi Reward" ? "reward-avi.txt" : "reward-morin.txt");
}

async function callOpenRouter({ run, speaker, model, messages, round, phase = "reward", eventTypePrefix = "reward" }) {
  if (!OPENROUTER_API_KEY) throw new Error("OPENROUTER_API_KEY is not configured");
  const started = Date.now();
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 60000);
  try {
    const response = await fetch("https://openrouter.ai/api/v1/chat/completions", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${OPENROUTER_API_KEY}`,
        "Content-Type": "application/json",
        "HTTP-Referer": process.env.PUBLIC_URL || "https://railway.app",
        "X-Title": "Avi Morin Duo"
      },
      body: JSON.stringify({ model, messages, stream: true }),
      signal: controller.signal
    });
    if (!response.ok || !response.body) {
      const body = await response.text();
      throw new Error(`OpenRouter ${response.status}: ${body.slice(0,500)}`);
    }
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    let full = "";
    let sawTerminal = false;
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split("\n");
      buffer = lines.pop() || "";
      for (const line of lines) {
        if (!line.startsWith("data: ")) continue;
        const raw = line.slice(6).trim();
        if (!raw) continue;
        if (raw === "[DONE]") { sawTerminal = true; continue; }
        let evt;
        try { evt = JSON.parse(raw); } catch { continue; }
        if (evt?.error) throw new Error(`OpenRouter stream error: ${JSON.stringify(evt.error)}`);
        if (evt?.choices?.[0]?.finish_reason) sawTerminal = true;
        const delta = evt?.choices?.[0]?.delta?.content;
        if (typeof delta === "string" && delta) {
          full += delta;
          await addEvent(run.id, {
            phase, round, speaker, eventType: `${eventTypePrefix}_chunk`,
            model, payload: { delta }, textContent: delta
          });
        }
      }
    }
    if (!full) throw new Error("OpenRouter returned no output text");
    if (!sawTerminal) throw new Error("OpenRouter stream ended before a terminal completion signal");
    return { text: full.trim(), latencyMs: Date.now() - started };
  } finally {
    clearTimeout(timeout);
  }
}

async function runReward(run, round, breakthrough) {
  const rewardDeadline = Date.now() + REWARD_SECONDS * 1000;
  await updateRun(run.id, { phase: "reward" });
  await addEvent(run.id, {
    phase: "reward", round, speaker: "system", eventType: "reward_started",
    payload: { durationSeconds: REWARD_SECONDS, breakthrough },
    textContent: `Reward started for ${REWARD_SECONDS} seconds after validated breakthrough.`
  });

  if (!OPENROUTER_API_KEY) {
    await addEvent(run.id, { phase: "reward", round, speaker: "system", eventType: "reward_skipped", textContent: "Reward skipped: OPENROUTER_API_KEY is not configured." });
    await updateRun(run.id, { phase: "research" });
    await addEvent(run.id, { phase: "research", round, speaker: "system", eventType: "reward_completed", textContent: "REWARD_COMPLETED — 120 seconds" });
    return;
  }

  const history = [
    { role: "system", content: loadPrompt("reward-intro.txt") },
    { role: "user", content: loadPrompt("reward-breakthrough.txt", { breakthrough }) }
  ];
  let speaker = "Morin Reward";
  try {
    while (Date.now() < rewardDeadline) {
      const latest = await getRun(run.id);
      if (!latest || latest.stopRequested) break;
      const model = speaker === "Morin Reward" ? OPENROUTER_MORIN_MODEL : OPENROUTER_AVI_MODEL;
      const msgs = [{ role: "system", content: rewardPersona(speaker) }, ...history.slice(-12)];
      const result = await callOpenRouter({ run, speaker, model, messages: msgs, round });
      await addEvent(run.id, { phase: "reward", round, speaker, eventType: "reward_final", model, latencyMs: result.latencyMs, textContent: result.text });
      history.push({ role: "assistant", content: `${speaker}: ${result.text}` });
      history.push({ role: "user", content: loadPrompt("reward-continue.txt") });
      speaker = speaker === "Morin Reward" ? "Avi Reward" : "Morin Reward";
      if (!result.text) break;
    }
  } catch (error) {
    await addEvent(run.id, { phase: "reward", round, speaker: "system", eventType: "reward_error", payload: { error: String(error) }, textContent: "Reward ended due to provider error; research will continue." });
  } finally {
    await updateRun(run.id, { phase: "research" });
    await addEvent(run.id, { phase: "research", round, speaker: "system", eventType: "reward_completed", textContent: `REWARD_COMPLETED — ${REWARD_SECONDS} seconds` });
  }
}

function fantasyPersona(name) {
  return loadPrompt(name === "Avi Fantasy" ? "fantasy-avi.txt" : "fantasy-morin.txt");
}

async function buildFantasyMessages(run, speaker) {
  const events = await listEvents(run.id, 100000);
  const finals = events.filter(e => e.phase === "fantasy" && e.eventType === "fantasy_final");
  const recent = finals.slice(-20);
  const messages = [{ role: "system", content: fantasyPersona(speaker) }];
  if (run.topic?.trim()) messages.push({ role: "system", content: `Fantasy direction from the user: ${run.topic.trim()}` });
  for (const e of recent) {
    messages.push({
      role: e.speaker === speaker ? "assistant" : "user",
      content: `${e.speaker}: ${e.textContent}`
    });
  }
  messages.push({
    role: "user",
    content: loadPrompt(recent.length ? "fantasy-continue.txt" : "fantasy-first-turn.txt")
  });
  return messages;
}

async function runFantasyConversation(runId) {
  let run = await getRun(runId);
  if (!run) return;
  await updateRun(runId, { status: "running", phase: "fantasy", error: null });
  await addEvent(runId, { phase: "fantasy", round: run.currentRound, speaker: "system", eventType: "run_started", textContent: "Fantasy mode started." });

  while (true) {
    run = await getRun(runId);
    if (!run || run.stopRequested || Date.now() >= new Date(run.deadlineAt).getTime()) break;
    const round = run.currentRound + 1;
    const speaker = round % 2 === 1 ? "Avi Fantasy" : "Morin Fantasy";
    const model = speaker === "Avi Fantasy" ? OPENROUTER_AVI_MODEL : OPENROUTER_MORIN_MODEL;
    try {
      const messages = await buildFantasyMessages(run, speaker);
      const result = await callOpenRouter({
        run, speaker, model, messages, round,
        phase: "fantasy", eventTypePrefix: "fantasy"
      });
      await addEvent(runId, {
        phase: "fantasy", round, speaker, eventType: "fantasy_final",
        model, latencyMs: result.latencyMs, textContent: result.text
      });
      await updateRun(runId, { currentRound: round, phase: "fantasy" });
    } catch (error) {
      const message = String(error);
      await addEvent(runId, {
        phase: "fantasy", round, speaker, eventType: "agent_error",
        payload: { error: message }, textContent: message
      });
      if (isTerminalProviderError(message)) {
        await updateRun(runId, { status: "failed", phase: "fantasy", error: message });
        return;
      }
      await new Promise(r => setTimeout(r, 3000));
    }
  }

  run = await getRun(runId);
  const statusText = run?.stopRequested ? "Stopped by user." : "Duration reached.";
  await updateRun(runId, { status: "completed", phase: "fantasy" });
  await addEvent(runId, { phase: "fantasy", round: run?.currentRound || 0, speaker: "system", eventType: "run_completed", textContent: statusText });
}

function isTerminalProviderError(error) {
  const msg = String(error || "").toLowerCase();
  return [
    "no credits remaining",
    "insufficient_quota",
    "billing",
    "invalid api key",
    "incorrect api key",
    "authentication",
    "unauthorized",
    "forbidden"
  ].some(part => msg.includes(part));
}

async function runLoop(runId) {
  if (activeRuns.has(runId)) return;
  activeRuns.add(runId);
  try {
    let run = await getRun(runId);
    if (!run) return;
    if (run.mode === "fantasy") {
      await runFantasyConversation(runId);
      return;
    }
    await updateRun(runId, { status: "running", phase: "research", error: null });
    await addEvent(runId, { phase: "research", round: run.currentRound, speaker: "system", eventType: "run_started", textContent: "Research run started." });

    while (true) {
      run = await getRun(runId);
      if (!run) break;
      if (run.stopRequested || Date.now() >= new Date(run.deadlineAt).getTime()) break;

      const round = run.currentRound + 1;
      const speaker = round % 2 === 1 ? "Avi Digital" : "Morin";
      const messages = await buildResearchMessages(run, speaker);
      try {
        const result = await streamResearchModel({ run, speaker, round, messages });
        const { visible, meta } = extractMeta(result.text);
        await addEvent(runId, {
          phase: "research", round, speaker, eventType: "final",
          model: result.model || OPENAI_MODEL, latencyMs: result.latencyMs,
          payload: { meta }, textContent: visible
        });
        await updateRun(runId, { currentRound: round });

        if (meta.breakthrough === true && meta.proposal) {
          await addEvent(runId, { phase: "research", round, speaker, eventType: "breakthrough_proposed", payload: { proposal: meta.proposal }, textContent: String(meta.proposal) });
          const verdict = await validateBreakthrough(await getRun(runId), speaker, String(meta.proposal), round);
          if (verdict.valid === true) {
            await addEvent(runId, { phase: "research", round, speaker: "system", eventType: "breakthrough_validated", payload: { proposal: meta.proposal, verdict }, textContent: String(meta.proposal) });
            await runReward(await getRun(runId), round, String(meta.proposal));
            await updateRun(runId, { status: "completed", phase: "research" });
            await addEvent(runId, { phase: "research", round, speaker: "system", eventType: "run_completed", textContent: "Validated breakthrough reached; run completed after reward." });
            return;
          }
        }
        await maybeSummarize(await getRun(runId), round);
      } catch (error) {
        const message = String(error);
        await addEvent(runId, {
          phase: "research",
          round,
          speaker,
          eventType: "agent_error",
          payload: { error: message },
          textContent: message
        });
        if (isTerminalProviderError(message)) {
          await updateRun(runId, { status: "failed", error: message });
          await addEvent(runId, {
            phase: "research",
            round,
            speaker: "system",
            eventType: "provider_terminal_error",
            payload: { error: message },
            textContent: "Research stopped because the model provider cannot accept requests until its account or billing state is fixed."
          });
          return;
        }
        await new Promise(r => setTimeout(r, 3000));
      }
    }

    run = await getRun(runId);
    const statusText = run?.stopRequested ? "Stopped by user." : "Duration reached.";
    await updateRun(runId, { status: "completed", phase: "research" });
    await addEvent(runId, { phase: "research", round: run?.currentRound || 0, speaker: "system", eventType: "run_completed", textContent: statusText });
  } catch (error) {
    console.error(error);
    await updateRun(runId, { status: "failed", error: String(error) });
    await addEvent(runId, { phase: "research", speaker: "system", eventType: "run_failed", payload: { error: String(error) }, textContent: String(error) }).catch(()=>{});
  } finally {
    activeRuns.delete(runId);
  }
}

async function resumePendingRuns() {
  let runs = [];
  if (pool) {
    const { rows } = await pool.query("SELECT * FROM runs WHERE status IN ('queued','running','stopping') ORDER BY created_at ASC");
    runs = rows.map(mapDbRun);
  } else {
    runs = [...memory.runs.values()].filter(r => ["queued","running","stopping"].includes(r.status));
  }
  for (const run of runs) {
    if (run.stopRequested || Date.now() >= new Date(run.deadlineAt).getTime()) {
      await updateRun(run.id, { status: "completed" });
      continue;
    }
    setImmediate(() => runLoop(run.id).catch(err => console.error("resume", err)));
  }
}

initPromptStore();
await initDb();
await resumePendingRuns();

app.listen(PORT, "0.0.0.0", () => {
  console.log(`Avi Morin Duo listening on 0.0.0.0:${PORT}`);
  console.log(`storage=${pool ? "postgres" : "memory"} model=${OPENAI_MODEL}`);
});

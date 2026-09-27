# Avi Morin Duo

A long-running two-agent research app.

## Core behavior
- Avi Digital and Morin are separate model calls with separate roles.
- Runs continue server-side after the browser closes.
- Full event log, chunk log, timing metadata and JSONL export.
- A proposed breakthrough must be validated by the other research agent.
- Reward mode is isolated from research and uses OpenRouter.
- Reward transcript never flows back into the OpenAI research context; research receives only a neutral completion marker.
- Runs resume after process restart when PostgreSQL is configured.

## Environment variables
- `OPENAI_API_KEY` — required
- `OPENAI_MODEL` — default `gpt-5.6-sol`
- `OPENROUTER_API_KEY` — optional until reward mode is used
- `OPENROUTER_AVI_MODEL` — default `openai/gpt-5.6`
- `OPENROUTER_MORIN_MODEL` — default `openai/gpt-5.6`
- `DATABASE_URL` — recommended for durable run history and restart recovery
- `APP_PIN` — strongly recommended before exposing the app publicly
- `REWARD_SECONDS` — default 120

## Health
`GET /health`

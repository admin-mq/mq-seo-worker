# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Run the worker
npm start

# Install dependencies
npm install
```

No test runner or linter is configured. The only script is `start`.

## Environment Variables

Required at runtime:
- `SUPABASE_URL` — Supabase project URL
- `SUPABASE_SERVICE_ROLE_KEY` — Supabase service role key

Optional (with defaults):
- `WORKER_ID` — Unique worker identifier (defaults to `RAILWAY_REPLICA_ID` or `worker-1`)
- `POLL_MS` — Job polling interval (default: 4000)
- `HEARTBEAT_MS` — Heartbeat update interval (default: 15000)
- `REQUEST_TIMEOUT_MS` — HTTP request timeout (default: 12000)
- `RESCUE_STALE_AFTER_MIN` — Minutes before a job is considered stale (default: 10)
- `MAX_REDIRECTS` — Max HTTP redirects (default: 5)

## Architecture

Single-file worker (`src/worker.js`, ~2250 lines, ES module) deployed to Railway. It polls Supabase for crawl jobs, crawls websites, extracts SEO signals, and writes results back to Supabase.

### Worker Loop

`main()` runs a continuous poll loop:
1. Call `rescueStaleJobs()` → Supabase RPC `scc_rescue_stale_jobs`
2. Call `claimNextJob()` → Supabase RPC `scc_claim_next_job`
3. If job claimed, run `runCrawlJob(job)`
4. Otherwise sleep `POLL_MS` and repeat

### Crawl Pipeline (`runCrawlJob`)

1. Fetch and process the seed URL (homepage)
2. Infer site type (`ecommerce`, `service`, `content`, `mixed`) from homepage links
3. Score and enqueue internal links with content-mix balancing
4. Process queue in score order (highest first), up to `maxPages`/`maxDepth`
5. For each page: fetch HTML → extract SEO data → score → generate actions → write to DB
6. Write crawl summary to snapshot; mark job complete

### Scoring (5 dimensions, all 0–100)

- **structural** — presence of title, meta description, H1, canonical, schema, 2xx status
- **visibility** — structural score × page-type weight × indexability × depth penalty
- **revenue** — page-type intent weight × 100
- **paid_risk** — intent weight + penalties for noindex, low structural/visibility, slow load
- **opportunity** — composite gap score: structural/visibility/content gaps × revenue potential

### Page Type Classification

`classifyPageTypeFromSignals()` maps each crawled URL to one of 16 types: `homepage`, `article`, `pricing`, `product`, `service`, `feature`, `conversion`, `archive`, `category`, `case_study`, `contact`, `about`, `location`, `proof`, `policy`, `general`.

Classification uses URL path patterns, title/H1 text, schema.org markup, and anchor text context.

### Content Mix Balancing (content-focused sites)

The queue manager enforces content diversity:
- Articles: ≥45% of crawl
- Archives: ≤25%
- Categories: ≤25%
- Max 2 pages per URL path family

### Database Tables

| Table | Purpose |
|---|---|
| `scc_crawl_jobs` | Job queue with worker assignment and progress |
| `scc_snapshots` | Crawl run metadata and status |
| `scc_pages` | Page records per site |
| `scc_page_snapshot_crawl` | Raw crawl data (status, title, meta, canonical, etc.) |
| `scc_page_snapshot_metrics` | Calculated scores |
| `scc_actions` | Generated SEO recommendations |

Supabase RPC functions used: `scc_claim_next_job`, `scc_rescue_stale_jobs`, `scc_job_heartbeat`, `scc_complete_crawl_job`.

## Deployment

Deployed on Railway via `railway.json` using NIXPACKS. Supports multiple concurrent replica instances — each claims jobs independently via `WORKER_ID`.

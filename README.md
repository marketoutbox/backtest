# Backtest Desk

Private Next.js control panel on Vercel, backed by a persistent Python importer and strategy runner. Upstox V3 candles are saved as separate Zstandard Parquet datasets in S3-compatible object storage. Postgres stores job status and import coverage. The browser never receives Upstox credentials or raw candle archives.

## What works

- Imports 1m, 5m, 15m, 30m, 1h, daily, weekly, and monthly candles directly from Upstox V3, one instrument per request.
- Resumable window imports: completed windows are skipped, and queued/running jobs resume on worker restart.
- Dedicated Parquet paths by interval/instrument/year/window; backtests load only matching objects and date ranges and reuse a worker-side disk cache for later runs.
- Two initial long-only strategies: dip below previous session close, and SMA crossover. Signals use completed candles, enter at the next candle open; stop/target checks start on the entry candle. A candle touching both is conservatively recorded as a stop. A gap through the exit level fills at the open. Open positions at the end of available data close at the final close.
- Job polling, archive coverage, summary metrics, trade table and CSV export.

## Deploy

1. Import the private `marketoutbox/backtest` repository in Vercel as a Next.js project. The root directory is the repository root.
2. Provision a PostgreSQL database and a private S3-compatible bucket. Deploy `worker/` as a persistent Docker service (for example a VM or managed container with an always-on process), reachable by Vercel. The worker is a separate service: Vercel alone does not run the long backfill jobs.
3. Copy `.env.worker.example` values into the worker service. Supply a server-side Upstox access token and `WORKER_SECRET`. Create the bucket before starting the worker.
4. Import and deploy with the default fail-closed middleware: without access settings every app route returns 503. In the Vercel project, enable **Vercel Authentication** for **All Deployments** (production and preview). Verify the protection, then set `SITE_ACCESS_MODE=vercel` for production and preview and redeploy. No additional site username or password is needed. Set `WORKER_URL` and matching `WORKER_SECRET` after the worker is running. An optional Basic Auth alternative uses both `SITE_USERNAME` and `SITE_PASSWORD`. Never set `SITE_ACCESS_MODE=vercel` without Vercel Authentication enabled on all deployments.
5. Deploy. Open the site, paste Upstox instrument keys (for example `NSE_EQ|INE002A01018`), choose dates and intervals, then import. Once completed, open Backtests. For subsequent imports select a later date range. Run imports only after market data has finalized.

Local UI: `npm install && npm run dev`. Local worker: `cd worker && pip install -r requirements.txt && uvicorn main:app --reload --port 8080`. You still need Postgres, S3, and an Upstox token to import real data. The worker's HTTP API requires `X-Worker-Secret`; keep its URL private where your provider allows.

## Important limits

Upstox V3 lists minute and hour history since January 2022 and daily or longer since January 2000, subject to instrument availability. The importer uses 28-day windows for up to 15 minutes, 85-day windows for 30 minutes/hourly, and 3000-day windows for daily. It defaults to 60 requests/minute and additionally caps requests at 1900 per rolling 30 minutes, below Upstox’s published 500/minute and 2000/30-minute limits. One worker process should be used; the limiter is process-local. The date-window coverage ledger tracks requests, not exchange trading-day completeness. Intraday/current-day V3 is not yet connected: schedule imports only after the day has moved into historical data, and verify freshness before trading research.

This first implementation reads selected Parquet objects per symbol into memory and caches them on the worker disk. Mount a persistent volume at `CACHE_DIR` for the fastest repeat runs; clear it if you manually replace archived objects outside the importer. It is suitable for a modest archive and a single worker, but large multi-year/1000-symbol sweeps need partition compaction, a distributed queue, and benchmarked parallel execution. Price series are unadjusted; handle splits and other corporate actions before interpreting long history. P/L omits costs, slippage, taxes, and portfolio capital concurrency. It does not simulate order-book liquidity or intrabar event order. Not an order execution service.

Deploy this directory as one persistent Python Docker process. Configure the variables in ../.env.worker.example, an S3-compatible bucket, and Postgres. Set WORKER_URL and the same WORKER_SECRET in Vercel. Keep the worker URL reachable from Vercel; its routes require X-Worker-Secret.

API Keys accepts Upstox Analytics Tokens (read-only, up to one year) and daily OAuth access tokens. The worker verifies the historical candle endpoint, encrypts saved tokens in Postgres using WORKER_SECRET, and rotates import requests across accounts with a rate limiter per account. The profile API may reject Analytics Tokens from a dynamic IP even while historical data works. Supply the account ID/UCC when adding an Analytics Token to group same-account tokens under one rate limit; otherwise its label is used. Daily OAuth tokens expire at 3:30 AM IST. Analytics Token expiry is not inferred; the user checks the exact date in Upstox, and a 401 marks it expired. Do not change WORKER_SECRET without replacing saved tokens because it is also the encryption key. UPSTOX_ACCESS_TOKEN also participates if it differs from saved tokens/accounts. IMPORT_WORKERS controls bounded parallel downloads in this single process.

Import jobs remain in Postgres and restart after a process restart. Run only one worker process because the in-process queue and rate limiter are not distributed.


## Download and restore a data backup

In **Data archive → Data backup → Download data backup**, select a parent folder on your PC using Chrome or Edge over HTTPS. The app creates `backtest-data-backup` there. Keep that whole folder. Parquet files retain their existing compression, so a roughly 7 GB archive remains roughly that size, plus small metadata/receipt files; no CSV expansion or second 7 GB server-side ZIP is needed.

Finish active jobs first. While a backup session is active, new jobs, resume, archive deletion and label edits return 409. Each successful request renews a five-minute lease. Cancel releases it; closing the tab or losing the connection releases it after the lease expires. This uses the existing **single-worker-process** deployment model. Never run multiple Uvicorn workers/replicas with this app. External changes to the archive or database must also be paused.

The browser fetches metadata in pages and prices in at most 2 MiB responses through authenticated Next.js routes. It writes directly to disk and checks SHA-256 for each chunk. Completed files have receipts. Retrying in the same parent folder verifies and reuses unchanged completed files; an interrupted file downloads again. A complete `manifest.json` is written only after all files succeed. Old extra files/receipts may remain after a resumed backup; the manifest identifies exactly what belongs to the completed snapshot.

The backup contains every indexed stock/timeframe, empty coverage windows, candle dates and instrument labels. It excludes API tokens, passwords, jobs/backtest history, unindexed files and the disposable read cache. It is an application data backup, not a full PostgreSQL dump.

### Verify on your PC

Use Python 3.12 and get `worker/restore_backup.py` from this repository. Verification needs only Python's standard library:

```sh
python worker/restore_backup.py "/path/to/backtest-data-backup"
```

A missing manifest means the download is incomplete. Any missing/truncated/corrupted file fails verification. Keep an additional independent copy of a completed backup if replacing the contents of the same backup folder.

### Restore to Railway / a replacement worker

1. Prepare an **empty destination archive database** and stop the destination worker during restore. The command refuses an existing coverage index or queued/running jobs; it never clears existing data. Keep the original service/data until the replacement is checked.
2. Install the dependencies in `worker/requirements.txt` on the machine running the restore. Configure the destination's `DATABASE_URL` and storage variables in that machine's environment; do not put secrets in commands committed to Git. The schema is created automatically.
3. For S3-compatible storage, run from your PC with the destination bucket credentials (`S3_BUCKET`, `S3_ENDPOINT_URL`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, `S3_REGION`) and an externally reachable Railway Postgres connection URL. The command uploads directly to the bucket and writes the matching Postgres index. Railway's internal database hostname is only reachable inside Railway.
4. For a Railway mounted-volume archive (`ARCHIVE_DIR`), first transfer the backup folder to a machine/container with access to that volume, then run the command there with the destination `ARCHIVE_DIR` and database URL. Setting `ARCHIVE_DIR` on your PC writes to your PC, not to Railway. Allow space for both the uploaded backup and restored files when using the same volume.
5. Run:

```sh
python worker/restore_backup.py "/path/to/backtest-data-backup" --restore
```

The entire backup is verified before connecting to the destination. Files upload under a fresh `restored/<id>/` prefix, and index rows commit together only after all files upload successfully. A failed restore leaves no committed partial index, but can leave unreferenced files under that attempt's prefix; remove those only after confirming the attempt failed, or use a fresh bucket/volume. Restoring after an interruption repeats the upload.

Start the worker with the same destination storage/database settings, check symbols/timeframes and sample prices in Data Viewer, then re-add Upstox tokens for future imports. Restoring the saved prices makes **no Upstox calls**. The backup button requires deploying both the updated Next.js app and Python worker. The restore is currently a command, not a browser upload button.

Tests: `python -m pytest worker/tests -q` (install pytest separately).

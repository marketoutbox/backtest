Deploy this directory as one persistent Python Docker process. Configure the variables in ../.env.worker.example, an S3-compatible bucket, and Postgres. Set WORKER_URL and the same WORKER_SECRET in Vercel. Keep the worker URL reachable from Vercel; its routes require X-Worker-Secret.

API Keys accepts Upstox Analytics Tokens (read-only, up to one year) and daily OAuth access tokens. The worker verifies the historical candle endpoint, encrypts saved tokens in Postgres using WORKER_SECRET, and rotates import requests across accounts with a rate limiter per account. The profile API may reject Analytics Tokens from a dynamic IP even while historical data works. Supply the account ID/UCC when adding an Analytics Token to group same-account tokens under one rate limit; otherwise its label is used. Daily OAuth tokens expire at 3:30 AM IST. Analytics Token expiry is not inferred; the user checks the exact date in Upstox, and a 401 marks it expired. Do not change WORKER_SECRET without replacing saved tokens because it is also the encryption key. UPSTOX_ACCESS_TOKEN also participates if it differs from saved tokens/accounts. IMPORT_WORKERS controls bounded parallel downloads in this single process.

Import jobs remain in Postgres and restart after a process restart. Run only one worker process because the in-process queue and rate limiter are not distributed.


## Background data backups

In **Data archive → Data backup**, click **Prepare data backup**. The worker records a durable backup job in Postgres, then creates one ZIP64 archive in the background. The page polls job status and displays **Download ZIP** when ready. You can close the page and return later; preparation does not depend on the browser. No folder picker or browser-side assembly is used.

Finish active jobs first. During preparation, new jobs, resume, archive deletion and label edits return 409. Once the ZIP is complete, the live archive can change without changing the prepared backup. The existing startup recovery requeues interrupted jobs; an interrupted backup is rebuilt. Keep the existing single-worker-process/replica deployment. External archive/database writers must also be paused during preparation.

The ZIP contains the existing compressed Parquet files, per-chunk SHA-256 checksums, coverage including empty windows, candle dates and instrument labels. Parquet is stored without recompression; a roughly 7 GB archive produces a roughly 7 GB ZIP. Credentials, jobs/backtest history, unindexed files and disposable read caches are excluded. This is a restorable application price-data backup, not a complete PostgreSQL dump.

### Storage and download

- **S3-compatible archive:** the worker writes ZIP bytes directly into an S3 multipart upload using bounded buffers. No extra full-size local ZIP is required. The completed backup occupies additional object storage under `backups/<job-id>.zip`. The storage credentials need multipart create/upload/complete/abort/list plus normal read/write/delete permissions. Configure a bucket lifecycle rule to abort abandoned multipart uploads as an additional cleanup measure.
- **Mounted-volume archive:** the worker creates the ZIP under `ARCHIVE_DIR/.backups` by default. Set `BACKUP_DIR` to another persistent mounted directory if desired. It checks available disk space before starting: allow approximately the archive size again, plus metadata headroom. Completed ZIPs are published by atomic rename; failed temporary files are removed.
- **Download:** the authenticated Next.js route obtains a short-lived download link and redirects the browser. The multi-GB ZIP never passes through a Vercel function or browser JavaScript buffer. S3 uses a one-hour presigned object URL. Local files use a one-hour HMAC-signed worker URL with native file/range serving. `WORKER_URL` and the storage endpoint must be browser-reachable HTTPS addresses for their respective download modes. Treat generated URLs as temporary bearer links; anyone holding one can download that backup until expiry. Neither link exposes `WORKER_SECRET`.
- **Resume:** where supported by the browser/storage, byte-range requests allow interrupted downloads to resume while the URL is valid. If a link expires, click Download ZIP again to obtain a fresh link. The prepared artifact remains available; no new Upstox download or backup preparation is needed.
- **Retention:** backups are not automatically deleted. The page shows the 20 most recent backup jobs. Use **Delete server backup** after saving your independent copy to reclaim space; it deletes that generated ZIP and its job record, not the stock archive. No permanent download URLs are stored in Postgres.

Deploy both the frontend and worker for this flow. Existing folder backups from the earlier implementation are still accepted by the restore script.

### Verify on your PC

Use Python 3.12 and `worker/restore_backup.py` from this repository. Verification needs only Python's standard library and reads the ZIP directly, without extracting another full copy:

```sh
python worker/restore_backup.py "/path/to/backtest-data-backup-ID.zip"
```

Missing, truncated or corrupted data fails verification. Keep the complete ZIP as your backup.

### Restore to Railway / a replacement worker

1. Prepare an **empty destination archive database** and stop the destination worker during restore. The command refuses an existing coverage index or queued/running jobs; it never clears existing data. Keep the original service/data until the replacement is checked.
2. Install `worker/requirements.txt` on the machine running the restore. Configure the destination's `DATABASE_URL` and storage variables in that machine's environment; do not commit secrets. The schema is created automatically.
3. For S3 storage, run from your PC using the destination bucket credentials and an externally reachable Railway Postgres connection URL. The command streams directly from the ZIP to the destination bucket and writes the matching Postgres index. Railway's internal database hostname is only reachable inside Railway.
4. For a Railway mounted-volume archive, transfer the ZIP to a machine/container with access to that volume and run the command there with the destination `ARCHIVE_DIR` and database URL. Setting `ARCHIVE_DIR` on your PC writes to your PC, not Railway. Allow space for the ZIP and the restored files when using the same volume.
5. Run:

```sh
python worker/restore_backup.py "/path/to/backtest-data-backup-ID.zip" --restore
```

All files are verified before connecting to the destination. Uploads use a fresh `restored/<id>/` prefix, and index rows commit together after every file uploads. A failed restore leaves no committed partial index but may leave unreferenced files under that attempt's prefix. Remove them only after confirming failure, or use a fresh destination. Retrying restore repeats uploads. Start the worker afterward, check Data Viewer and re-add API tokens for future imports. Restoration makes no Upstox calls. Browser upload restoration is not included.

Validation: `python -m pytest worker/tests -q` (install pytest separately), `node scripts/test-backup-picker.cjs` for the server-backup UI regression checks, and `npm run build`.

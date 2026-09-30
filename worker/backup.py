"""Durable background ZIP64 backups and direct, scoped download links."""
import hashlib
import hmac
import io
import json
import os
from pathlib import Path
import shutil
import threading
import time
import uuid
import zipfile
from contextlib import contextmanager
from datetime import datetime, timezone
from functools import wraps

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse

CHUNK_SIZE = 2 * 1024 * 1024
PART_SIZE = 8 * 1024 * 1024
COLUMNS = ('instrument', 'interval', 'from_date', 'to_date', 'rows', 'object_key', 'first_candle', 'last_candle')


def read_chunk(source):
    # Network streams may return short reads before EOF; checksum boundaries stay fixed.
    parts = bytearray()
    while len(parts) < CHUNK_SIZE:
        block = source.read(CHUNK_SIZE - len(parts))
        if not block: break
        parts.extend(block)
    return bytes(parts)


class MultipartWriter(io.RawIOBase):
    """Non-seekable ZIP sink: upload directly to S3 using bounded memory."""
    def __init__(self, client, bucket, key, filename):
        self.client, self.bucket, self.key = client, bucket, key
        self.upload_id = client.create_multipart_upload(Bucket=bucket, Key=key, ContentType='application/zip',
            ContentDisposition=f'attachment; filename="{filename}"')['UploadId']
        self.buffer = bytearray()
        self.parts = []
        self.position = 0

    def writable(self): return True
    def seekable(self): return False
    def tell(self): return self.position

    def write(self, data):
        self.position += len(data)
        self.buffer.extend(data)
        while len(self.buffer) >= PART_SIZE:
            self._part(bytes(self.buffer[:PART_SIZE]))
            del self.buffer[:PART_SIZE]
        return len(data)

    def _part(self, data):
        if len(self.parts) >= 10000:
            raise RuntimeError('Backup exceeds multipart limit; increase PART_SIZE')
        number = len(self.parts) + 1
        result = self.client.upload_part(Bucket=self.bucket, Key=self.key, UploadId=self.upload_id, PartNumber=number, Body=data)
        self.parts.append({'PartNumber': number, 'ETag': result['ETag']})

    def complete(self):
        if self.buffer:
            self._part(bytes(self.buffer)); self.buffer.clear()
        self.client.complete_multipart_upload(Bucket=self.bucket, Key=self.key, UploadId=self.upload_id,
            MultipartUpload={'Parts': self.parts})

    def abort(self):
        self.client.abort_multipart_upload(Bucket=self.bucket, Key=self.key, UploadId=self.upload_id)


class ArchiveBackup:
    def __init__(self, db, archive_path, storage, bucket):
        self.db, self.archive_path, self.storage, self.bucket = db, archive_path, storage, bucket
        self.lock = threading.RLock()

    def mutation(self, function):
        @wraps(function)
        def wrapped(*args, **kwargs):
            with self.lock:
                with self.db() as conn:
                    active = conn.execute("SELECT 1 FROM jobs WHERE kind='backup' AND status IN ('queued','running') LIMIT 1").fetchone()
                if active:
                    raise HTTPException(409, 'A backup is being prepared on the server. Wait until it finishes before changing the archive.')
                return function(*args, **kwargs)
        return wrapped

    def start(self, enqueue):
        with self.lock:
            with self.db() as conn:
                if conn.execute("SELECT 1 FROM jobs WHERE status IN ('queued','running') LIMIT 1").fetchone():
                    raise HTTPException(409, 'Wait for active imports, backtests or backups to finish first.')
                if not conn.execute('SELECT 1 FROM coverage LIMIT 1').fetchone():
                    raise HTTPException(400, 'The archive is empty.')
            return enqueue()

    def list(self):
        with self.db() as conn:
            rows = conn.execute("SELECT id,status,progress,total,error,created_at,result,details FROM jobs WHERE kind='backup' ORDER BY created_at DESC LIMIT 20").fetchall()
        return {'backups': [dict(zip(('id','status','progress','total','error','created_at','result','details'), row)) for row in rows]}

    def directory(self):
        root = os.environ.get('BACKUP_DIR')
        if not root:
            archive = os.environ.get('ARCHIVE_DIR')
            if not archive: raise RuntimeError('ARCHIVE_DIR or BACKUP_DIR is required for local backup files')
            root = str(Path(archive) / '.backups')
        return Path(root).resolve()

    @contextmanager
    def source(self, key, client):
        local = self.archive_path(key)
        if local:
            with local.open('rb') as stream:
                yield stream, local.stat().st_size
        else:
            result = client.get_object(Bucket=self.bucket(), Key=key)
            try:
                yield result['Body'], result['ContentLength']
            finally:
                result['Body'].close()

    def build(self, job_id, update):
        uuid.UUID(job_id)
        with self.db() as conn:
            conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            rows = conn.execute('SELECT ' + ','.join(COLUMNS) + ' FROM coverage ORDER BY instrument,interval,from_date,to_date').fetchall()
            names = conn.execute('SELECT instrument,symbol FROM instrument_names ORDER BY instrument').fetchall()
        coverage = json.loads(json.dumps([dict(zip(COLUMNS, row)) for row in rows], default=str))
        windows = [row for row in coverage if row['rows'] > 0]
        filename = f'backtest-data-backup-{job_id}.zip'
        local_mode = bool(os.environ.get('ARCHIVE_DIR'))
        client = None if local_mode else self.storage()
        key = f'backups/{job_id}.zip'
        target = partial = None
        if local_mode:
            folder = self.directory(); folder.mkdir(parents=True, exist_ok=True)
            target = folder / filename
            partial = folder / (filename + '.part')
            partial.unlink(missing_ok=True)
            total_bytes = sum(self.archive_path(row['object_key']).stat().st_size for row in windows)
            required = total_bytes + max(64 * 1024 * 1024, len(coverage) * 2048)
            if shutil.disk_usage(folder).free < required:
                raise RuntimeError(f'Not enough backup disk space. Need approximately {required / 1024**3:.2f} GiB free. Add storage or set BACKUP_DIR to a larger mounted volume.')
            sink = partial.open('wb')  # restarted jobs replace their unfinished attempt
        else:
            # Clean up only this job's abandoned multipart upload after a restart.
            pages = client.get_paginator('list_multipart_uploads').paginate(Bucket=self.bucket(), Prefix=key)
            for page in pages:
                for upload in page.get('Uploads', []):
                    if upload['Key'] == key:
                        client.abort_multipart_upload(Bucket=self.bucket(), Key=key, UploadId=upload['UploadId'])
            sink = MultipartWriter(client, self.bucket(), key, filename)
            total_bytes = None
        processed = 0; files = []; last_update = 0
        update(job_id, progress=0, total=len(windows), details={'stage': 'archiving', 'bytes': 0, 'total_bytes': total_bytes})
        try:
            with zipfile.ZipFile(sink, 'w', compression=zipfile.ZIP_STORED, allowZip64=True) as archive:
                for index, row in enumerate(windows):
                    path = 'files/' + hashlib.sha256(row['object_key'].encode()).hexdigest() + '.parquet'
                    hashes = []; size = 0
                    with self.source(row['object_key'], client) as (source, expected):
                        if expected <= 0: raise RuntimeError('An indexed price file is empty')
                        with archive.open('backtest-data-backup/' + path, 'w', force_zip64=True) as output:
                            while True:
                                data = read_chunk(source)
                                if not data: break
                                output.write(data); hashes.append(hashlib.sha256(data).hexdigest())
                                size += len(data); processed += len(data)
                                if time.monotonic() - last_update > 1:
                                    update(job_id, progress=index, details={'stage': 'archiving', 'bytes': processed, 'total_bytes': total_bytes})
                                    last_update = time.monotonic()
                    if size != expected:
                        raise RuntimeError(f'Archive file changed or was truncated: {row["object_key"]}')
                    files.append({'key': row['object_key'], 'path': path, 'size': size, 'chunks': hashes})
                manifest = {'format': 'backtest-desk-archive', 'version': 1, 'chunk_size': CHUNK_SIZE,
                    'created_at': datetime.now(timezone.utc).isoformat(), 'coverage': coverage,
                    'instrument_names': [dict(instrument=a, symbol=b) for a,b in names], 'files': files}
                # Serialize metadata incrementally too; do not pass a huge buffer to the multipart sink.
                with archive.open('backtest-data-backup/manifest.json', 'w', force_zip64=True) as output:
                    for piece in json.JSONEncoder().iterencode(manifest): output.write(piece.encode())
            update(job_id, progress=len(windows), details={'stage': 'finalizing', 'bytes': processed, 'total_bytes': total_bytes})
            size = sink.tell()
            if local_mode:
                sink.flush(); os.fsync(sink.fileno()); sink.close(); partial.replace(target)
            else:
                sink.complete()
            return {'storage': 'local' if local_mode else 's3', 'object_key': key,
                    'filename': filename, 'size': size, 'files': len(files), 'windows': len(coverage)}
        except BaseException:
            if local_mode:
                sink.close(); partial.unlink(missing_ok=True)
            else:
                try: sink.abort()
                except Exception: pass
            raise

    def completed(self, job_id):
        try: uuid.UUID(job_id)
        except ValueError as exc: raise HTTPException(404, 'Backup not found') from exc
        with self.db() as conn:
            row = conn.execute("SELECT status,result FROM jobs WHERE id=%s AND kind='backup'", (job_id,)).fetchone()
        if not row: raise HTTPException(404, 'Backup not found')
        if row[0] != 'complete': raise HTTPException(409, 'Backup is not ready')
        return row[1]

    def signature(self, job_id, expires):
        secret = os.environ.get('WORKER_SECRET')
        if not secret: raise HTTPException(503, 'Worker secret is not configured')
        return hmac.new(secret.encode(), f'backup-download-v1:{job_id}:{expires}'.encode(), hashlib.sha256).hexdigest()

    def link(self, job_id):
        result = self.completed(job_id)
        if result['storage'] == 's3':
            client = self.storage()
            client.head_object(Bucket=self.bucket(), Key=result['object_key'])
            return {'url': client.generate_presigned_url('get_object', Params={'Bucket': self.bucket(), 'Key': result['object_key'],
                'ResponseContentDisposition': f'attachment; filename="{result["filename"]}"'}, ExpiresIn=3600)}
        path = self.directory() / result['filename']
        if not path.is_file(): raise HTTPException(404, 'Backup file is missing. Prepare a new backup.')
        expires = int(time.time()) + 3600
        return {'url': f'/backup/{job_id}/download?expires={expires}&signature={self.signature(job_id, expires)}'}

    def download(self, job_id, expires, signature):
        if expires < int(time.time()) or not hmac.compare_digest(signature, self.signature(job_id, expires)):
            raise HTTPException(403, 'Download link expired or invalid. Click Download again on the backup page.')
        result = self.completed(job_id)
        if result['storage'] != 'local': raise HTTPException(404, 'Use the storage download link')
        path = self.directory() / result['filename']
        if not path.is_file(): raise HTTPException(404, 'Backup file is missing')
        return FileResponse(path, filename=result['filename'], media_type='application/zip', headers={'Cache-Control': 'private, no-store', 'Referrer-Policy': 'no-referrer'})

    def delete(self, job_id):
        with self.lock:
            result = self.completed(job_id)
            if result['storage'] == 'local': (self.directory() / result['filename']).unlink(missing_ok=True)
            else: self.storage().delete_object(Bucket=self.bucket(), Key=result['object_key'])
            with self.db() as conn:
                conn.execute("DELETE FROM jobs WHERE id=%s AND kind='backup' AND status='complete'", (job_id,))
        return {'deleted': True}

    def router(self, auth, enqueue):
        router = APIRouter(prefix='/backup')
        @router.post('', dependencies=[Depends(auth)])
        def start(): return self.start(enqueue)
        @router.get('', dependencies=[Depends(auth)])
        def listing(): return self.list()
        @router.get('/{job_id}/link', dependencies=[Depends(auth)])
        def link(job_id: str): return self.link(job_id)
        @router.delete('/{job_id}', dependencies=[Depends(auth)])
        def delete(job_id: str): return self.delete(job_id)
        # This endpoint authenticates only the scoped, expiring download signature.
        @router.get('/{job_id}/download')
        def download(job_id: str, expires: int, signature: str): return self.download(job_id, expires, signature)
        return router

"""Bounded archive downloads. The app intentionally runs one worker process."""
import hashlib
import json
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from functools import wraps

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response

CHUNK_SIZE = 2 * 1024 * 1024
PAGE_SIZE = 200
LEASE_SECONDS = 300
COLUMNS = ('instrument', 'interval', 'from_date', 'to_date', 'rows', 'object_key', 'first_candle', 'last_candle')


class ArchiveBackup:
    def __init__(self, db, archive_path, storage, bucket):
        self.db, self.archive_path, self.storage, self.bucket = db, archive_path, storage, bucket
        self.lock = threading.RLock()
        self.session = None

    def active(self):
        if self.session and self.session['until'] <= time.monotonic():
            self.session = None
        return self.session

    def mutation(self, function):
        @wraps(function)
        def wrapped(*args, **kwargs):
            with self.lock:
                if self.active():
                    raise HTTPException(409, 'A data backup is downloading. Finish or cancel it before changing the archive.')
                return function(*args, **kwargs)
        return wrapped

    @contextmanager
    def use(self, session_id):
        with self.lock:
            session = self.active()
            if not session or session['id'] != session_id:
                raise HTTPException(410, 'Backup session ended. Download again into the same folder to resume.')
            try:
                yield session
            finally:
                session['until'] = time.monotonic() + LEASE_SECONDS

    def start(self):
        with self.lock:
            if self.active():
                raise HTTPException(409, 'A backup is already downloading. Cancel it or wait five minutes after its last request.')
            with self.db() as conn:
                if conn.execute("SELECT 1 FROM jobs WHERE status IN ('queued','running') LIMIT 1").fetchone():
                    raise HTTPException(409, 'Wait for imports and backtests to finish before downloading a backup.')
                rows = conn.execute('SELECT ' + ','.join(COLUMNS) + ' FROM coverage ORDER BY instrument,interval,from_date,to_date').fetchall()
                names = conn.execute('SELECT instrument,symbol FROM instrument_names ORDER BY instrument').fetchall()
            if not rows:
                raise HTTPException(400, 'The archive is empty.')
            # JSON conversion also normalizes dates for the portable manifest.
            coverage = json.loads(json.dumps([dict(zip(COLUMNS, row)) for row in rows], default=str))
            self.session = {'id': uuid.uuid4().hex, 'until': time.monotonic() + LEASE_SECONDS,
                            'created_at': datetime.now(timezone.utc).isoformat(), 'coverage': coverage,
                            'names': [dict(instrument=a, symbol=b) for a, b in names]}
            return {'id': self.session['id'], 'format': 'backtest-desk-archive', 'version': 1,
                    'created_at': self.session['created_at'], 'windows': len(coverage),
                    'files': sum(row['rows'] > 0 for row in coverage), 'chunk_size': CHUNK_SIZE,
                    'page_size': PAGE_SIZE, 'name_count': len(names)}

    def page(self, session_id, offset):
        with self.use(session_id) as session:
            return {'coverage': session['coverage'][offset:offset + PAGE_SIZE],
                    'instrument_names': session['names'][offset:offset + PAGE_SIZE]}

    def chunk(self, session_id, index, offset):
        with self.use(session_id) as session:
            if index >= len(session['coverage']):
                raise HTTPException(404, 'Backup file not found')
            row = session['coverage'][index]
            if row['rows'] <= 0:
                raise HTTPException(404, 'Empty coverage window has no file')
            key = row['object_key']
            path = self.archive_path(key)
            if path:
                stat = path.stat()
                size = stat.st_size
                version = hashlib.sha256(f'{stat.st_ino}:{stat.st_mtime_ns}:{size}'.encode()).hexdigest()
                if offset >= size:
                    raise HTTPException(416, 'Invalid backup offset')
                with path.open('rb') as source:
                    source.seek(offset)
                    data = source.read(min(CHUNK_SIZE, size - offset))
            else:
                client = self.storage()
                head = client.head_object(Bucket=self.bucket(), Key=key)
                size = head['ContentLength']
                version = head['ETag']
                if offset >= size:
                    raise HTTPException(416, 'Invalid backup offset')
                result = client.get_object(Bucket=self.bucket(), Key=key, IfMatch=version,
                                           Range=f'bytes={offset}-{min(size, offset + CHUNK_SIZE) - 1}')
                body = result['Body']
                try:
                    data = body.read(CHUNK_SIZE)
                finally:
                    body.close()
            if len(data) != min(CHUNK_SIZE, size - offset):
                raise HTTPException(502, 'Archive file was truncated during backup')
            return Response(data, media_type='application/octet-stream', headers={
                'Cache-Control': 'no-store', 'X-Backup-Size': str(size), 'X-Backup-Version': version,
                'X-Backup-SHA256': hashlib.sha256(data).hexdigest()})

    def finish(self, session_id):
        with self.use(session_id):
            self.session = None
        return {'released': True}

    def router(self, auth):
        router = APIRouter(prefix='/backup', dependencies=[Depends(auth)])
        router.add_api_route('', self.start, methods=['POST'])

        @router.get('/{session_id}/manifest')
        def manifest(session_id: str, offset: int = Query(0, ge=0)):
            return self.page(session_id, offset)

        @router.get('/{session_id}/files/{index}')
        def file_chunk(session_id: str, index: int, offset: int = Query(0, ge=0)):
            if index < 0:
                raise HTTPException(404, 'Backup file not found')
            return self.chunk(session_id, index, offset)

        @router.delete('/{session_id}')
        def release(session_id: str):
            return self.finish(session_id)

        return router

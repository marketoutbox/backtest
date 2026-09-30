import asyncio
from datetime import date, datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import sys
import time
import uuid
import zipfile

import pytest
from fastapi import FastAPI, Header, HTTPException
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from backup import ArchiveBackup, MultipartWriter, CHUNK_SIZE
from restore_backup import verify_backup, restore


class Result:
    def __init__(self, rows): self.rows = rows
    def fetchone(self): return self.rows[0] if self.rows else None
    def fetchall(self): return self.rows


class Database:
    def __init__(self, rows): self.rows, self.jobs = rows, {}
    def __call__(self, *args): return self
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def execute(self, sql, params=None):
        if sql.startswith('SELECT status,result'):
            job = self.jobs.get(params[0]); return Result([(job['status'], job.get('result'))] if job else [])
        if sql.startswith('SELECT kind,payload'):
            job = self.jobs[params[0]]; return Result([(job['kind'], {})])
        if sql.startswith('SELECT 1 FROM jobs'):
            return Result([(1,)] if any(j['status'] in ('queued','running') and ("kind='backup'" not in sql or j['kind']=='backup') for j in self.jobs.values()) else [])
        if sql.startswith('SELECT id,status'):
            return Result([(id, j['status'], j.get('progress',0), j.get('total',0), j.get('error'), datetime.now(timezone.utc), j.get('result'), j.get('details',{})) for id,j in self.jobs.items() if j['kind']=='backup'])
        if sql.startswith('SELECT 1 FROM coverage'): return Result([(1,)] if self.rows else [])
        if 'FROM coverage' in sql: return Result(self.rows)
        if sql.startswith('SELECT instrument,symbol'): return Result([('NSE_EQ|TEST', 'TEST')])
        if sql.startswith('DELETE FROM jobs'): self.jobs.pop(params[0])
        return Result([])
    def update(self, id, **fields): self.jobs[id].update(fields)


@pytest.fixture
def fixture(tmp_path, monkeypatch):
    monkeypatch.setenv('ARCHIVE_DIR', str(tmp_path / 'archive'))
    monkeypatch.setenv('WORKER_SECRET', 'test-worker-secret')
    key = 'candles/test.parquet'; path = tmp_path / 'archive' / key
    path.parent.mkdir(parents=True); data = b'p' * CHUNK_SIZE + b'end'; path.write_bytes(data)
    db = Database([('NSE_EQ|TEST','1m','2026-01-01','2026-01-28',20,key,'2026-01-02','2026-01-27'),
                   ('NSE_EQ|TEST','1m','2026-02-01','2026-02-28',0,'candles/empty.parquet',None,None)])
    manager = ArchiveBackup(db, lambda key: tmp_path / 'archive' / key, None, None)
    def enqueue():
        id = str(uuid.uuid4()); db.jobs[id] = {'kind':'backup', 'status':'queued'}; return {'id':id, 'status':'queued'}
    def auth(x_worker_secret: str = Header('')):
        if x_worker_secret != 'test': raise HTTPException(401)
    app = FastAPI(); app.include_router(manager.router(auth, enqueue))
    return manager, TestClient(app), db, data, tmp_path


def complete(manager, db):
    job_id = str(uuid.uuid4()); db.jobs[job_id] = {'kind':'backup', 'status':'running'}
    result = manager.build(job_id, db.update)
    db.update(job_id, status='complete', result=result)
    return job_id, result


def test_background_zip_roundtrip_and_normal_range_download(fixture):
    manager, client, db, data, root = fixture
    headers = {'X-Worker-Secret':'test'}
    assert client.post('/backup').status_code == 401
    job = client.post('/backup', headers=headers).json(); id = job['id']
    # The start response only queues work; there is no open browser connection during build.
    assert not manager.directory().exists()
    db.update(id, status='running')
    result = manager.build(id, db.update); db.update(id, status='complete', result=result)
    archive = manager.directory() / result['filename']
    manifest, size = verify_backup(archive)
    assert size == len(data) and len(manifest['coverage']) == 2 and len(manifest['files']) == 1
    with zipfile.ZipFile(archive) as zipped:
        assert zipped.read('backtest-data-backup/' + manifest['files'][0]['path']) == data
        assert all(info.compress_type == zipfile.ZIP_STORED for info in zipped.infolist())
    assert client.get(f'/backup/{id}/link').status_code == 401
    link = client.get(f'/backup/{id}/link', headers=headers).json()['url']
    assert 'test-worker-secret' not in link
    response = client.get(link, headers={'Range':'bytes=0-99'})
    assert response.status_code == 206 and len(response.content) == 100
    assert response.content == archive.read_bytes()[:100]
    assert 'attachment' in response.headers['content-disposition']
    assert client.get(link.replace('signature=', 'signature=bad')).status_code == 403
    expired = int(time.time()) - 1
    assert client.get(f'/backup/{id}/download?expires={expired}&signature={manager.signature(id, expired)}').status_code == 403
    assert client.get('/backup', headers=headers).json()['backups'][0]['status'] == 'complete'
    assert client.delete(f'/backup/{id}').status_code == 401
    assert client.delete(f'/backup/{id}', headers=headers).status_code == 200
    assert not archive.exists() and (root / 'archive/candles/test.parquet').exists()


def test_queue_dispatch_builds_without_browser(fixture, monkeypatch):
    import main as worker
    manager, client, db, data, root = fixture
    id = str(uuid.uuid4()); db.jobs[id] = {'kind':'backup', 'status':'queued'}
    monkeypatch.setattr(worker, 'db', db); monkeypatch.setattr(worker, 'update', db.update)
    monkeypatch.setattr(worker, 'archive_backup', manager)
    async def run():
        monkeypatch.setattr(worker, 'queue', asyncio.Queue())
        await worker.queue.put(id)
        task = asyncio.create_task(worker.run_queue())
        await asyncio.wait_for(worker.queue.join(), timeout=10)
        task.cancel()
        try: await task
        except asyncio.CancelledError: pass
    asyncio.run(run())
    assert db.jobs[id]['status'] == 'complete'
    assert (manager.directory() / db.jobs[id]['result']['filename']).is_file()


def test_persistent_mutation_guards_and_active_job_rejection(fixture):
    manager, client, db, data, root = fixture
    id = str(uuid.uuid4()); db.jobs[id] = {'kind':'backfill','status':'running'}
    with pytest.raises(HTTPException): manager.start(lambda: None)
    db.jobs[id]['kind'] = 'backup'
    called = []
    operation = manager.mutation(lambda: called.append(1))
    with pytest.raises(HTTPException): operation()
    # Guard survives manager recreation because it uses Postgres job state, not browser leases.
    fresh = ArchiveBackup(db, manager.archive_path, None, None)
    with pytest.raises(HTTPException): fresh.mutation(lambda: None)()
    db.jobs[id]['status'] = 'failed'; operation(); assert called == [1]


class S3:
    def __init__(self, objects): self.objects, self.parts, self.aborted = objects, {}, []
    def create_multipart_upload(self, **kwargs): self.key = kwargs['Key']; return {'UploadId':'upload'}
    def upload_part(self, **kwargs): self.parts[kwargs['PartNumber']] = kwargs['Body']; return {'ETag':str(kwargs['PartNumber'])}
    def complete_multipart_upload(self, **kwargs): self.objects[kwargs['Key']] = b''.join(self.parts[i] for i in sorted(self.parts))
    def abort_multipart_upload(self, **kwargs): self.aborted.append(kwargs['UploadId'])
    def get_object(self, **kwargs):
        data = self.objects[kwargs['Key']]; return {'ContentLength':len(data), 'Body':io.BytesIO(data)}
    def head_object(self, **kwargs): return {'ContentLength':len(self.objects[kwargs['Key']])}
    def get_paginator(self, name): return self
    def paginate(self, **kwargs): return [{}]
    def generate_presigned_url(self, operation, **kwargs):
        assert operation == 'get_object' and kwargs['ExpiresIn'] == 3600
        return 'https://storage.example/backup.zip?signed=true'


def test_s3_build_streams_multipart_and_returns_storage_link(fixture, monkeypatch):
    manager, client, db, data, root = fixture
    monkeypatch.delenv('ARCHIVE_DIR')
    import backup
    monkeypatch.setattr(backup, 'PART_SIZE', 1024*1024)
    s3 = S3({'candles/test.parquet':data})
    manager.archive_path = lambda key: None; manager.storage = lambda: s3; manager.bucket = lambda: 'bucket'
    id, result = complete(manager, db)
    assert len(s3.parts) >= 2 and not s3.aborted
    assert max(map(len, s3.parts.values())) <= 1024*1024
    downloaded = root / 'download.zip'; downloaded.write_bytes(s3.objects[result['object_key']])
    assert verify_backup(downloaded)[1] == len(data)
    assert manager.link(id)['url'].startswith('https://storage.example/')
    assert not (root / 'archive/.backups').exists()


def test_failure_cleans_partial_zip_and_multipart(fixture, monkeypatch):
    manager, client, db, data, root = fixture
    (root / 'archive/candles/test.parquet').unlink()
    with pytest.raises(FileNotFoundError): complete(manager, db)
    assert not list(manager.directory().glob('*.part'))
    monkeypatch.delenv('ARCHIVE_DIR')
    s3 = S3({}); manager.archive_path = lambda key: None
    manager.storage = lambda: s3; manager.bucket = lambda: 'bucket'
    with pytest.raises(KeyError): complete(manager, db)
    assert s3.aborted == ['upload']


def test_low_disk_space_fails_before_backup(fixture, monkeypatch):
    import backup
    from types import SimpleNamespace
    manager, client, db, data, root = fixture
    monkeypatch.setattr(backup.shutil, 'disk_usage', lambda path: SimpleNamespace(free=1))
    with pytest.raises(RuntimeError, match='Not enough backup disk'): complete(manager, db)
    assert not list(manager.directory().glob('*.zip'))


def test_real_parquet_restores_from_zip_and_existing_target_is_rejected(fixture, monkeypatch):
    import polars as pl
    import psycopg
    import main as worker
    manager, client, db, data, root = fixture
    frame = pl.DataFrame({'ts':[datetime(2026,1,2,4,tzinfo=timezone.utc)], 'session_date':[date(2026,1,2)],
                          'open':[100.0], 'high':[110.0], 'low':[90.0], 'close':[105.0], 'volume':[100], 'open_interest':[0]})
    frame.write_parquet(root / 'archive/candles/test.parquet', compression='zstd')
    id, result = complete(manager, db)
    zipped = manager.directory() / result['filename']; manifest, _ = verify_backup(zipped)
    inserted = []
    class Target:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def execute(self, sql, params=None):
            if sql.startswith('SELECT 1 FROM coverage'): return Result([(1,)] if inserted else [])
            if sql.startswith('INSERT INTO coverage'): inserted.append(params)
            if sql.startswith('SELECT object_key'): return Result([(inserted[0][5],)])
            return Result([])
    monkeypatch.setattr(psycopg, 'connect', lambda url: Target())
    monkeypatch.setenv('DATABASE_URL','test-only'); monkeypatch.setenv('ARCHIVE_DIR',str(root / 'restored'))
    monkeypatch.setenv('CACHE_DIR',str(root / 'cache'))
    restore(zipped, manifest)
    assert len(inserted) == 2  # includes the empty coverage window
    monkeypatch.setattr(worker,'db',Target)
    assert worker.load_candles('NSE_EQ|TEST','1m',date(2026,1,1),date(2026,1,28)).to_dicts() == frame.to_dicts()
    with pytest.raises(ValueError, match='empty archive'): restore(zipped, manifest)


def test_corrupt_or_incomplete_zip_rejected(fixture):
    manager, client, db, data, root = fixture
    id, result = complete(manager, db)
    zipped = manager.directory() / result['filename']
    with zipfile.ZipFile(zipped) as source:
        entries = {name:source.read(name) for name in source.namelist()}
    price = next(name for name in entries if name.endswith('.parquet'))
    entries[price] = b'x' + entries[price][1:]
    corrupt = root / 'corrupt.zip'
    with zipfile.ZipFile(corrupt,'w') as out:
        for name, content in entries.items(): out.writestr(name,content)
    with pytest.raises(ValueError, match='checksum'): verify_backup(corrupt)
    incomplete = root / 'incomplete.zip'
    with zipfile.ZipFile(incomplete,'w') as out: out.writestr(price,data)
    with pytest.raises(KeyError): verify_backup(incomplete)


def test_network_short_reads_preserve_checksum_boundaries():
    from backup import read_chunk
    class Fragmented(io.BytesIO):
        def read(self, length=-1): return super().read(min(length, 8192))
    payload = b'a' * (CHUNK_SIZE + 17)
    source = Fragmented(payload)
    assert read_chunk(source) == payload[:CHUNK_SIZE]
    assert read_chunk(source) == payload[CHUNK_SIZE:]
    assert read_chunk(source) == b''

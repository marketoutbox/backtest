import hashlib
import io
import json
import sys
import time
from pathlib import Path

import pytest
from fastapi import FastAPI, HTTPException, Header
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from backup import ArchiveBackup, CHUNK_SIZE
from restore_backup import verify_backup


class Result:
    def __init__(self, rows): self.rows = rows
    def fetchone(self): return self.rows[0] if self.rows else None
    def fetchall(self): return self.rows


class Database:
    def __init__(self, rows): self.rows, self.jobs = rows, []
    def __call__(self): return self
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def execute(self, sql):
        if 'FROM jobs' in sql: return Result(self.jobs)
        if 'FROM coverage' in sql: return Result(self.rows)
        return Result([('NSE_EQ|TEST', 'TEST')])


@pytest.fixture
def fixture(tmp_path):
    key = 'candles/test.parquet'
    data = b'p' * CHUNK_SIZE + b'last chunk'
    path = tmp_path / key; path.parent.mkdir(); path.write_bytes(data)
    database = Database([('NSE_EQ|TEST', '1m', '2026-01-01', '2026-01-28', 20, key, '2026-01-02', '2026-01-27'),
                         ('NSE_EQ|TEST', '1m', '2026-02-01', '2026-02-28', 0, 'candles/empty.parquet', None, None)])
    manager = ArchiveBackup(database, lambda key: tmp_path / key, None, None)
    app = FastAPI()
    def auth(x_worker_secret: str = Header('')):
        if x_worker_secret != 'test': raise HTTPException(401)
    app.include_router(manager.router(auth))
    return manager, TestClient(app), database, data, tmp_path


def test_local_roundtrip_and_integrity(fixture):
    manager, client, database, data, root = fixture
    headers = {'X-Worker-Secret': 'test'}
    assert client.post('/backup').status_code == 401
    session = client.post('/backup', headers=headers).json()
    sid = session['id']
    page = client.get(f'/backup/{sid}/manifest', headers=headers).json()
    assert session['files'] == 1 and session['windows'] == 2
    result = bytearray(); hashes = []
    for offset in range(0, len(data), CHUNK_SIZE):
        response = client.get(f'/backup/{sid}/files/0?offset={offset}', headers=headers)
        assert response.status_code == 200
        assert len(response.content) <= CHUNK_SIZE
        assert response.headers['x-backup-sha256'] == hashlib.sha256(response.content).hexdigest()
        hashes.append(response.headers['x-backup-sha256']); result.extend(response.content)
    assert bytes(result) == data
    assert client.get(f'/backup/{sid}/files/1', headers=headers).status_code == 404
    assert client.get(f'/backup/{sid}/files/0?offset={len(data)}', headers=headers).status_code == 416
    assert client.get(f'/backup/{sid}/files/-1', headers=headers).status_code == 404
    assert client.get(f'/backup/{sid}/manifest').status_code == 401
    assert client.delete(f'/backup/{sid}').status_code == 401
    out = root / 'download'; (out / 'files').mkdir(parents=True)
    (out / 'files/test.parquet').write_bytes(result)
    manifest = {**session, **page, 'files': [{'key': 'candles/test.parquet', 'path': 'files/test.parquet', 'size': len(result), 'chunks': hashes}]}
    (out / 'manifest.json').write_text(json.dumps(manifest))
    assert verify_backup(out)[1] == len(data)
    (out / 'files/test.parquet').write_bytes(b'x' + result[1:])
    with pytest.raises(ValueError, match='checksum'): verify_backup(out)
    assert client.delete(f'/backup/{sid}', headers=headers).status_code == 200
    assert client.get(f'/backup/{sid}/manifest', headers=headers).status_code == 410


def test_job_and_mutation_guards(fixture):
    manager, client, database, data, root = fixture
    database.jobs = [(1,)]
    with pytest.raises(HTTPException) as error: manager.start()
    assert error.value.status_code == 409
    database.jobs = []
    sid = manager.start()['id']
    with pytest.raises(HTTPException): manager.start()
    calls = []
    mutate = manager.mutation(lambda: calls.append(1))
    with pytest.raises(HTTPException): mutate()
    assert calls == []
    with pytest.raises(HTTPException): manager.finish('wrong-session')
    manager.finish(sid); mutate(); assert calls == [1]
    manager.start(); manager.session['until'] = time.monotonic() - 1
    mutate(); assert calls == [1, 1]


def test_s3_range_read_closes_body(fixture):
    manager, client, database, data, root = fixture
    bodies = []
    class S3:
        def head_object(self, **kwargs): return {'ContentLength': len(data), 'ETag': 'version1'}
        def get_object(self, **kwargs):
            assert kwargs['IfMatch'] == 'version1'
            begin, end = map(int, kwargs['Range'][6:].split('-'))
            stream = io.BytesIO(data[begin:end + 1]); bodies.append(stream)
            return {'Body': stream}
    manager.archive_path = lambda key: None
    manager.storage = S3; manager.bucket = lambda: 'test'
    sid = manager.start()['id']
    result = manager.chunk(sid, 0, CHUNK_SIZE)
    assert result.body == b'last chunk' and bodies[-1].closed


def test_paginated_metadata_and_expiry(fixture):
    manager, client, database, data, root = fixture
    database.rows = database.rows * 201
    sid = manager.start()['id']
    assert len(manager.page(sid, 0)['coverage']) == 200
    assert len(manager.page(sid, 400)['coverage']) == 2
    assert manager.page(sid, 0)['instrument_names'] == [{'instrument': 'NSE_EQ|TEST', 'symbol': 'TEST'}]
    manager.session['until'] = time.monotonic() - 1
    with pytest.raises(HTTPException) as error: manager.chunk(sid, 0, 0)
    assert error.value.status_code == 410


def test_incomplete_or_unsafe_backup_rejected(tmp_path):
    base = {'format': 'backtest-desk-archive', 'version': 1, 'chunk_size': CHUNK_SIZE,
            'coverage': [{'instrument': 'NSE_EQ|TEST', 'interval': '1m', 'from_date': '2026-01-01',
                          'to_date': '2026-01-28', 'rows': 1, 'object_key': 'candles/test.parquet',
                          'first_candle': None, 'last_candle': None}], 'instrument_names': [], 'files': []}
    path = tmp_path / 'manifest.json'
    path.write_text(json.dumps(base))
    with pytest.raises(ValueError, match='inventory'): verify_backup(tmp_path)
    base['files'] = [{'key': 'candles/test.parquet', 'path': 'files/../../secret', 'size': 1, 'chunks': ['bad']}]
    path.write_text(json.dumps(base))
    with pytest.raises(ValueError, match='Unsafe'): verify_backup(tmp_path)


def test_missing_source_never_returns_success(fixture):
    manager, client, database, data, root = fixture
    sid = manager.start()['id']
    (root / 'candles/test.parquet').unlink()
    with pytest.raises(FileNotFoundError): manager.chunk(sid, 0, 0)


def test_restored_parquet_loads_through_existing_reader(tmp_path, monkeypatch):
    from datetime import datetime, date, timezone
    import polars as pl
    import psycopg
    import main as worker
    from restore_backup import restore
    source = tmp_path / 'backup'; (source / 'files').mkdir(parents=True)
    frame = pl.DataFrame({'ts': [datetime(2026, 1, 2, 4, tzinfo=timezone.utc)],
                          'session_date': [date(2026, 1, 2)], 'open': [100.0], 'high': [110.0],
                          'low': [90.0], 'close': [105.0], 'volume': [100], 'open_interest': [0]})
    file = source / 'files/test.parquet'; frame.write_parquet(file, compression='zstd')
    payload = file.read_bytes()
    manifest = {'format': 'backtest-desk-archive', 'version': 1, 'chunk_size': CHUNK_SIZE,
                'coverage': [{'instrument': 'NSE_EQ|TEST', 'interval': '1m', 'from_date': '2026-01-01',
                              'to_date': '2026-01-28', 'rows': 1, 'object_key': 'candles/test.parquet',
                              'first_candle': '2026-01-02', 'last_candle': '2026-01-02'}],
                'instrument_names': [{'instrument': 'NSE_EQ|TEST', 'symbol': 'TEST'}],
                'files': [{'key': 'candles/test.parquet', 'path': 'files/test.parquet', 'size': len(payload),
                           'chunks': [hashlib.sha256(payload).hexdigest()]}]}
    (source / 'manifest.json').write_text(json.dumps(manifest))
    checked, size = verify_backup(source)
    inserted = []
    class Target:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def execute(self, sql, params=None):
            if sql.startswith('INSERT INTO coverage'): inserted.append(params)
            if sql.startswith('SELECT object_key'): return Result([(inserted[0][5],)])
            return Result([])
    monkeypatch.setattr(psycopg, 'connect', lambda url: Target())
    monkeypatch.setenv('DATABASE_URL', 'test-only')
    monkeypatch.setenv('ARCHIVE_DIR', str(tmp_path / 'restored'))
    monkeypatch.setenv('CACHE_DIR', str(tmp_path / 'cache'))
    restore(source, checked)
    assert len(inserted) == 1 and inserted[0][5].startswith('restored/')
    monkeypatch.setattr(worker, 'db', Target)
    loaded = worker.load_candles('NSE_EQ|TEST', '1m', date(2026, 1, 1), date(2026, 1, 28))
    assert loaded.to_dicts() == frame.to_dicts()


def test_restore_refuses_existing_archive_before_upload(tmp_path, monkeypatch):
    import psycopg
    from restore_backup import restore
    class Target:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def execute(self, sql, params=None): return Result([(1,)] if sql.startswith('SELECT 1 FROM coverage') else [])
    monkeypatch.setattr(psycopg, 'connect', lambda url: Target())
    monkeypatch.setenv('DATABASE_URL', 'test-only')
    target = tmp_path / 'target'
    monkeypatch.setenv('ARCHIVE_DIR', str(target))
    with pytest.raises(ValueError, match='empty archive'):
        restore(tmp_path, {'files': [], 'coverage': [], 'instrument_names': []})
    assert not target.exists()

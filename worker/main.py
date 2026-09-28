"""Persistent Upstox candle importer and bar-based backtest API."""
import asyncio
import base64
import csv
import io
import hashlib
import heapq
from pathlib import Path
import json
import os
import re
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from urllib.parse import quote

import boto3
import httpx
import polars as pl
import psycopg
from cryptography.fernet import Fernet
from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, field_validator

IST = ZoneInfo('Asia/Kolkata')
INTERVALS = {'1m': ('minutes', 1, 28), '5m': ('minutes', 5, 28), '15m': ('minutes', 15, 28), '30m': ('minutes', 30, 85), '1h': ('hours', 1, 85), '1d': ('days', 1, 3000), '1w': ('weeks', 1, 8000), '1mo': ('months', 1, 8000)}
START = date(2022, 1, 1)
TOKEN_PROBE_URL = 'https://api.upstox.com/v3/historical-candle/NSE_EQ%7CINE002A01018/days/1/2025-01-02/2025-01-01'
app = FastAPI(title='Backtest Desk worker')
queue: asyncio.Queue[str] = asyncio.Queue()
rate_locks: dict[str, asyncio.Lock] = {}
rate_windows: dict[str, list[float]] = {}
rate_halfhours: dict[str, list[float]] = {}
rate_seconds: dict[str, list[float]] = {}
rate_cooldowns: dict[str, float] = {}
token_cursor = 0
environment_profile_cache: dict[str, object] = {}

def db():
    url = os.environ.get('DATABASE_URL')
    if not url: raise RuntimeError('DATABASE_URL is required')
    return psycopg.connect(url)

def storage():
    return boto3.client('s3', endpoint_url=os.environ.get('S3_ENDPOINT_URL') or None,
        aws_access_key_id=os.environ.get('S3_ACCESS_KEY_ID'), aws_secret_access_key=os.environ.get('S3_SECRET_ACCESS_KEY'),
        region_name=os.environ.get('S3_REGION') or 'auto')

def bucket():
    value = os.environ.get('S3_BUCKET')
    if not value: raise RuntimeError('S3_BUCKET is required')
    return value

def archive_path(key):
    root = os.environ.get('ARCHIVE_DIR')
    if not root: return None
    base = Path(root).resolve()
    path = (base / key).resolve()
    if not path.is_relative_to(base): raise ValueError('Invalid archive key')
    return path

def auth(x_worker_secret: str | None = Header(None)):
    expected = os.environ.get('WORKER_SECRET')
    if not expected or x_worker_secret != expected: raise HTTPException(401, 'Unauthorized')

def validate_key(value: str):
    if not re.fullmatch(r'[A-Z0-9_]+\|[A-Za-z0-9 ._-]+|[A-Za-z0-9&_.-]{1,50}', value): raise ValueError('Enter an NSE ticker such as RELIANCE or an Upstox instrument key such as NSE_EQ|INE002A01018')
    return value

async def resolve_instruments(values: list[str], names: dict[str, str] | None = None):
    resolved = []
    token_pool = available_tokens()
    async with httpx.AsyncClient(timeout=20) as client:
        for value in values:
            if '|' in value:
                resolved.append(value)
                continue
            if not token_pool: raise HTTPException(400, 'Add a valid access token in API Keys')
            symbol = value.upper()
            response = None
            for account in token_pool:
                try:
                    response = await client.get('https://api.upstox.com/v2/instruments/search',
                        params={'query': symbol, 'exchanges': 'NSE', 'segments': 'EQ', 'records': 30},
                        headers={'Authorization': f'Bearer {account["token"]}', 'Accept': 'application/json'})
                except httpx.RequestError as exc:
                    raise HTTPException(502, f'Upstox instrument lookup failed for {symbol}: {exc.__class__.__name__}') from exc
                if response.status_code != 401: break
            if response.status_code == 401: raise HTTPException(400, 'All Upstox tokens were rejected. Replace them in API Keys.')
            if response.status_code != 200: raise HTTPException(502, f'Upstox instrument lookup failed for {symbol} (HTTP {response.status_code})')
            matches = [item['instrument_key'] for item in response.json().get('data', [])
                if item.get('segment') == 'NSE_EQ' and item.get('trading_symbol', '').upper() == symbol and item.get('instrument_key')]
            matches = list(dict.fromkeys(matches))
            if not matches: raise HTTPException(400, f'No exact NSE equity ticker found for {symbol}. Enter an Upstox instrument key for other instruments.')
            if len(matches) > 1: raise HTTPException(400, f'Multiple NSE equities match {symbol}. Enter the specific Upstox instrument key.')
            if names is not None: names[matches[0]] = symbol
            resolved.append(matches[0])
    return list(dict.fromkeys(resolved))

class Backfill(BaseModel):
    instruments: list[str] = Field(min_length=1, max_length=1000)
    intervals: list[str] = Field(min_length=1)
    from_date: date
    to_date: date
    refresh_existing: bool = False
    @field_validator('instruments')
    @classmethod
    def keys(cls, values): return list(dict.fromkeys(validate_key(s.strip()) for s in values))
    @field_validator('intervals')
    @classmethod
    def periods(cls, values):
        if any(v not in INTERVALS for v in values): raise ValueError(f'Intervals: {list(INTERVALS)}')
        return list(dict.fromkeys(values))

class Backtest(BaseModel):
    instruments: list[str] = Field(min_length=1, max_length=1000)
    interval: str
    from_date: date
    to_date: date
    strategy: str = 'dip_buy'
    dip_pct: float = Field(default=3, gt=0, le=50)
    target_pct: float = Field(default=2, gt=0, le=100)
    stop_pct: float = Field(default=1, gt=0, le=100)
    fast: int = Field(default=10, ge=2, le=1000)
    slow: int = Field(default=30, ge=3, le=2000)
    capital_per_trade: float = Field(default=10000, gt=0)
    @field_validator('instruments')
    @classmethod
    def keys(cls, values): return list(dict.fromkeys(validate_key(s.strip()) for s in values))

SCHEMA = '''
CREATE TABLE IF NOT EXISTS jobs (id text PRIMARY KEY, kind text NOT NULL, status text NOT NULL, payload jsonb NOT NULL, result jsonb, error text, progress integer NOT NULL DEFAULT 0, total integer NOT NULL DEFAULT 0, created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS coverage (instrument text NOT NULL, interval text NOT NULL, from_date date NOT NULL, to_date date NOT NULL, rows integer NOT NULL, object_key text NOT NULL, updated_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (instrument, interval, from_date, to_date));
CREATE TABLE IF NOT EXISTS instrument_names (instrument text PRIMARY KEY, symbol text NOT NULL);
CREATE TABLE IF NOT EXISTS api_tokens (id text PRIMARY KEY, label text NOT NULL, account_id text NOT NULL UNIQUE, encrypted_token text NOT NULL, expires_at timestamptz NOT NULL, status text NOT NULL DEFAULT 'ready', created_at timestamptz NOT NULL DEFAULT now());
ALTER TABLE api_tokens ALTER COLUMN expires_at DROP NOT NULL;
ALTER TABLE api_tokens ADD COLUMN IF NOT EXISTS token_type text NOT NULL DEFAULT 'oauth';
ALTER TABLE coverage ADD COLUMN IF NOT EXISTS first_candle date;
ALTER TABLE coverage ADD COLUMN IF NOT EXISTS last_candle date;
'''

@app.on_event('startup')
async def startup():
    with db() as conn:
        for statement in SCHEMA.strip().split(';'):
            if statement.strip(): conn.execute(statement)
        for row in conn.execute("SELECT id FROM jobs WHERE status IN ('queued','running') ORDER BY created_at"):
            conn.execute("UPDATE jobs SET status='queued' WHERE id=%s", (row[0],))
            await queue.put(row[0])
    app.state.runner = asyncio.create_task(run_queue())
    app.state.metadata_runner = asyncio.create_task(backfill_coverage_dates())

@app.on_event('shutdown')
async def shutdown():
    app.state.runner.cancel()
    app.state.metadata_runner.cancel()

def fill_coverage_date(key, instrument, interval, begin, end):
    path = archive_path(key)
    if path:
        frame = pl.read_parquet(path, columns=['session_date'])
    else:
        payload = storage().get_object(Bucket=bucket(), Key=key)['Body'].read()
        frame = pl.read_parquet(io.BytesIO(payload), columns=['session_date'])
    if frame.is_empty(): return
    first, last = frame['session_date'].min(), frame['session_date'].max()
    with db() as conn:
        conn.execute('UPDATE coverage SET first_candle=%s,last_candle=%s WHERE instrument=%s AND interval=%s AND from_date=%s AND to_date=%s AND first_candle IS NULL', (first,last,instrument,interval,begin,end))

async def backfill_coverage_dates():
    with db() as conn:
        rows = conn.execute('SELECT object_key,instrument,interval,from_date,to_date FROM coverage WHERE rows>0 AND first_candle IS NULL').fetchall()
    for row in rows:
        try: await asyncio.to_thread(fill_coverage_date,*row)
        except asyncio.CancelledError: raise
        except Exception: continue

def token_cipher():
    secret = os.environ.get('WORKER_SECRET')
    if not secret: raise RuntimeError('WORKER_SECRET is required to encrypt access tokens')
    return Fernet(base64.urlsafe_b64encode(hashlib.sha256(('upstox-token-v1:' + secret).encode()).digest()))

def token_expiry():
    now = datetime.now(IST)
    cutoff = now.replace(hour=3, minute=30, second=0, microsecond=0)
    if now >= cutoff: cutoff += timedelta(days=1)
    return cutoff

def environment_account():
    token = os.environ.get('UPSTOX_ACCESS_TOKEN')
    if not token: return None
    digest = hashlib.sha256(token.encode()).hexdigest()
    if environment_profile_cache.get('digest') == digest and environment_profile_cache.get('until', 0) > time.monotonic():
        return environment_profile_cache.get('account_id')
    account_id = None
    try:
        with httpx.Client(timeout=8) as client:
            response = client.get('https://api.upstox.com/v2/user/profile', headers={'Authorization': f'Bearer {token}', 'Accept': 'application/json'})
        if response.status_code == 200: account_id = response.json().get('data', {}).get('user_id')
        else:
            with httpx.Client(timeout=8) as client:
                candle_response = client.get(TOKEN_PROBE_URL, headers={'Authorization': f'Bearer {token}', 'Accept': 'application/json'})
            if candle_response.status_code == 200:
                account_id = 'ANALYTICS:' + digest[:12]
    except (httpx.RequestError, ValueError): pass
    environment_profile_cache.update(digest=digest, account_id=account_id, until=time.monotonic()+300)
    return account_id

class AddToken(BaseModel):
    label: str = Field(min_length=1, max_length=60)
    access_token: str = Field(min_length=20, max_length=4096)
    account_id: str | None = None

@app.get('/api-keys', dependencies=[Depends(auth)])
def list_api_keys():
    with db() as conn:
        rows = conn.execute('SELECT id,label,account_id,expires_at,status,token_type FROM api_tokens ORDER BY created_at').fetchall()
    saved = [{'id': a, 'label': b, 'account_id': c, 'expires_at': d.isoformat() if d else None, 'status': e, 'token_type': kind} for a,b,c,d,e,kind in rows]
    if os.environ.get('UPSTOX_ACCESS_TOKEN'):
        account_id = environment_account()
        used = any(item['id'] == 'environment' for item in available_tokens())
        saved.append({'id': 'environment', 'label': 'Railway access token', 'account_id': account_id or 'Unverified', 'expires_at': None, 'status': 'environment' if account_id or not rows else 'unverified', 'token_type': 'environment', 'used': used})
    return {'keys': saved, 'saved_count': len(rows), 'requests_per_minute_per_account': min(int(os.environ.get('MAX_REQUESTS_PER_MINUTE', '450')), 480), 'parallel_workers': min(16, max(2, int(os.environ.get('IMPORT_WORKERS', '8'))))}

@app.post('/api-keys', dependencies=[Depends(auth)])
async def add_api_key(body: AddToken):
    token = body.access_token.strip()
    headers = {'Authorization': f'Bearer {token}', 'Accept': 'application/json'}
    async with httpx.AsyncClient(timeout=15) as client:
        # The profile API may require a static IP for Analytics Tokens; verify candle access independently.
        profile, probe = await asyncio.gather(
            client.get('https://api.upstox.com/v2/user/profile', headers=headers),
            client.get(TOKEN_PROBE_URL, headers=headers), return_exceptions=True)
    if isinstance(probe, httpx.RequestError):
        raise HTTPException(502, f'Could not verify Upstox candle access: {probe.__class__.__name__}') from probe
    if isinstance(profile, httpx.RequestError): profile = None
    if probe.status_code != 200:
        raise HTTPException(400, f'Upstox historical candle API returned HTTP {probe.status_code}; this token could not be verified for downloads. Profile API returned HTTP {profile.status_code if profile else "unreachable"}.')
    profile_data = profile.json().get('data', {}) if profile is not None and profile.status_code == 200 else {}
    token_type = 'oauth' if profile_data.get('user_id') else 'analytics'
    account_id = profile_data['user_id'] if token_type == 'oauth' else (body.account_id or body.label).strip().upper()
    if not re.fullmatch(r'[A-Z0-9 _.-]{1,60}', account_id): raise HTTPException(400, 'Enter a valid account ID or label')
    expires_at = token_expiry() if token_type == 'oauth' else None
    encrypted = token_cipher().encrypt(token.encode()).decode()
    with db() as conn:
        row = conn.execute("INSERT INTO api_tokens (id,label,account_id,encrypted_token,expires_at,status,token_type) VALUES (%s,%s,%s,%s,%s,'ready',%s) ON CONFLICT (account_id) DO UPDATE SET label=excluded.label, encrypted_token=excluded.encrypted_token, expires_at=excluded.expires_at, status='ready', token_type=excluded.token_type RETURNING id", (str(uuid.uuid4()),body.label.strip(),account_id,encrypted,expires_at,token_type)).fetchone()
    return {'id': row[0], 'label': body.label.strip(), 'account_id': account_id, 'token_type': token_type}

@app.delete('/api-keys/{key_id}', dependencies=[Depends(auth)])
def delete_api_key(key_id: str):
    with db() as conn:
        if conn.execute("SELECT 1 FROM jobs WHERE status IN ('queued','running') LIMIT 1").fetchone():
            raise HTTPException(409, 'Wait for active jobs to finish before removing a token')
        deleted = conn.execute('DELETE FROM api_tokens WHERE id=%s RETURNING id', (key_id,)).fetchone()
    if not deleted: raise HTTPException(404, 'Token not found')
    return {'deleted': True}

def available_tokens():
    with db() as conn:
        rows = conn.execute("SELECT id,account_id,encrypted_token FROM api_tokens WHERE (expires_at IS NULL OR expires_at > now()) AND status='ready' ORDER BY created_at").fetchall()
        saved_count = conn.execute('SELECT count(*) FROM api_tokens').fetchone()[0]
    tokens = []
    if rows:
        cipher = token_cipher()
        tokens = [{'id': a, 'account_id': b, 'token': cipher.decrypt(c.encode()).decode()} for a,b,c in rows]
    if os.environ.get('UPSTOX_ACCESS_TOKEN'):
        account_id = environment_account()
        duplicate_token = any(row['token'] == os.environ['UPSTOX_ACCESS_TOKEN'] for row in tokens)
        if not duplicate_token and account_id and account_id not in {row['account_id'] for row in tokens}:
            tokens.append({'id': 'environment', 'account_id': account_id, 'token': os.environ['UPSTOX_ACCESS_TOKEN']})
        elif not duplicate_token and not saved_count:
            tokens.append({'id': 'environment', 'account_id': 'environment', 'token': os.environ['UPSTOX_ACCESS_TOKEN']})
    return tokens

@app.get('/status', dependencies=[Depends(auth)])
def status():
    with db() as conn:
        rows = conn.execute('SELECT interval, count(DISTINCT instrument), sum(rows) FROM coverage GROUP BY interval').fetchall()
        running = conn.execute("SELECT count(*) FROM jobs WHERE status IN ('queued','running')").fetchone()[0]
    return {'archive': [{'interval': x, 'instruments': y, 'candles': z} for x,y,z in rows], 'active_jobs': running, 'intervals': list(INTERVALS)}

@app.get('/symbols', dependencies=[Depends(auth)])
def symbols():
    with db() as conn:
        missing = conn.execute('SELECT DISTINCT c.instrument FROM coverage c LEFT JOIN instrument_names n ON n.instrument=c.instrument WHERE n.instrument IS NULL LIMIT 10').fetchall()
        token_pool = available_tokens()
        token = token_pool[0]['token'] if token_pool else None
        if token and missing:
            with httpx.Client(timeout=6) as client:
                for (key,) in missing:
                    if not key.startswith('NSE_EQ|'): continue
                    try:
                        response = client.get('https://api.upstox.com/v2/instruments/search',
                            params={'query': key.split('|', 1)[1], 'exchanges': 'NSE', 'segments': 'EQ', 'records': 30},
                            headers={'Authorization': f'Bearer {token}', 'Accept': 'application/json'})
                        if response.status_code == 200:
                            match = next((item for item in response.json().get('data', []) if str(item.get('instrument_key', '')).upper() == key.upper()), None)
                            if match and match.get('trading_symbol'):
                                conn.execute('INSERT INTO instrument_names (instrument,symbol) VALUES (%s,%s) ON CONFLICT (instrument) DO UPDATE SET symbol=excluded.symbol', (key, match['trading_symbol']))
                    except (httpx.RequestError, ValueError): pass
        rows = conn.execute('SELECT c.instrument, c.interval, min(c.from_date), max(c.to_date), sum(c.rows), count(*), n.symbol, min(c.first_candle), max(c.last_candle), count(*) FILTER (WHERE c.rows > 0), count(*) FILTER (WHERE c.rows > 0 AND c.first_candle IS NOT NULL) FROM coverage c LEFT JOIN instrument_names n ON n.instrument=c.instrument GROUP BY c.instrument,c.interval,n.symbol ORDER BY coalesce(n.symbol,c.instrument),c.interval').fetchall()
    return {'symbols': [{'instrument': a, 'interval': b, 'from_date': str(c), 'to_date': str(d), 'candles': e, 'windows': w, 'symbol': name or a, 'first_candle': str(first) if first else None, 'last_candle': str(last) if last else None, 'filled_windows': filled, 'verified_windows': verified} for a,b,c,d,e,w,name,first,last,filled,verified in rows]}

class InstrumentLabel(BaseModel):
    instrument: str
    symbol: str

@app.post('/instrument/label', dependencies=[Depends(auth)])
def set_instrument_label(body: InstrumentLabel):
    symbol = body.symbol.strip().upper()
    if not re.fullmatch(r'[A-Z0-9&_.-]{1,50}', symbol): raise HTTPException(400, 'Enter a valid ticker name')
    with db() as conn:
        if not conn.execute('SELECT 1 FROM coverage WHERE instrument=%s LIMIT 1', (body.instrument,)).fetchone():
            raise HTTPException(404, 'Instrument not found in archive')
        conn.execute('INSERT INTO instrument_names (instrument,symbol) VALUES (%s,%s) ON CONFLICT (instrument) DO UPDATE SET symbol=excluded.symbol', (body.instrument, symbol))
    return {'instrument': body.instrument, 'symbol': symbol}

def submit(kind, payload):
    job_id = str(uuid.uuid4())
    with db() as conn:
        conn.execute('INSERT INTO jobs (id,kind,status,payload) VALUES (%s,%s,%s,%s::jsonb)', (job_id, kind, 'queued', json.dumps(payload)))
    queue.put_nowait(job_id)
    return {'id': job_id, 'status': 'queued'}

@app.post('/backfill', dependencies=[Depends(auth)])
async def backfill(body: Backfill):
    if body.from_date > body.to_date or body.to_date > datetime.now(IST).date(): raise HTTPException(400, 'Invalid date range')
    if body.from_date < START and any(INTERVALS[p][0] in ('minutes','hours') for p in body.intervals):
        raise HTTPException(400, 'Upstox minute and hourly history starts in January 2022. Select 2022-01-01 or later for intraday imports.')
    names = {}
    body.instruments = await resolve_instruments(body.instruments, names)
    with db() as conn:
        for key, label in names.items():
            conn.execute('INSERT INTO instrument_names (instrument,symbol) VALUES (%s,%s) ON CONFLICT (instrument) DO UPDATE SET symbol=excluded.symbol', (key, label))
    return submit('backfill', body.model_dump(mode='json'))

@app.post('/backtests', dependencies=[Depends(auth)])
async def backtests(body: Backtest):
    if body.interval not in INTERVALS or body.strategy not in ('dip_buy','sma_cross'): raise HTTPException(400, 'Unknown interval or strategy')
    if body.from_date > body.to_date or body.to_date > datetime.now(IST).date(): raise HTTPException(400, 'Invalid date range')
    if body.fast >= body.slow: raise HTTPException(400, 'Fast SMA must be shorter than slow SMA')
    body.instruments = await resolve_instruments(body.instruments)
    return submit('backtest', body.model_dump(mode='json'))

@app.get('/jobs/{job_id}', dependencies=[Depends(auth)])
def job(job_id: str):
    with db() as conn:
        row = conn.execute('SELECT id,kind,status,progress,total,result,error FROM jobs WHERE id=%s', (job_id,)).fetchone()
    if row is None: raise HTTPException(404, 'Job not found')
    return dict(zip(('id','kind','status','progress','total','result','error'), row))

def update(job_id, **fields):
    with db() as conn:
        for col, value in fields.items():
            if col not in ('status','progress','total','result','error'): raise ValueError(col)
            conn.execute(f'UPDATE jobs SET {col}=%s, updated_at=now() WHERE id=%s', (json.dumps(value) if col == 'result' else value, job_id))

async def run_queue():
    while True:
        job_id = await queue.get()
        try:
            with db() as conn:
                kind, payload = conn.execute('SELECT kind,payload FROM jobs WHERE id=%s', (job_id,)).fetchone()
            update(job_id, status='running', error=None)
            result = await ingest(job_id, payload) if kind == 'backfill' else await asyncio.to_thread(run_backtest, job_id, payload)
            update(job_id, status='complete', result=result)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            update(job_id, status='failed', error=(str(exc) or exc.__class__.__name__)[:1000])
        finally:
            queue.task_done()

async def throttle(account_id):
    lock = rate_locks.setdefault(account_id, asyncio.Lock())
    async with lock:
        limit = min(int(os.environ.get('MAX_REQUESTS_PER_MINUTE', '450')), 480)
        minute = rate_windows.setdefault(account_id, [])
        halfhour = rate_halfhours.setdefault(account_id, [])
        second = rate_seconds.setdefault(account_id, [])
        while True:
            now = time.monotonic()
            minute[:] = [v for v in minute if now - v < 60]
            halfhour[:] = [v for v in halfhour if now - v < 1800]
            second[:] = [v for v in second if now - v < 1]
            cooldown = max(0, rate_cooldowns.get(account_id, 0) - now)
            if len(second) < 45 and len(minute) < limit and len(halfhour) < 1900 and not cooldown:
                minute.append(now); halfhour.append(now); second.append(now)
                return
            wait_minute = 60 - (now - minute[0]) if len(minute) >= limit else 0
            wait_halfhour = 1800 - (now - halfhour[0]) if len(halfhour) >= 1900 else 0
            wait_second = 1 - (now - second[0]) if len(second) >= 45 else 0
            await asyncio.sleep(max(0.05, cooldown, wait_minute, wait_halfhour, wait_second))

def next_token(tokens):
    global token_cursor
    ready = [token for token in tokens if not token.get('invalid')]
    if not ready: raise RuntimeError('All Upstox access tokens expired or were rejected. Add fresh tokens in API Keys.')
    token = ready[token_cursor % len(ready)]
    token_cursor += 1
    return token

def windows(first: date, last: date, span: int):
    cur = first
    while cur <= last:
        end = min(last, cur + timedelta(days=span - 1))
        yield cur, end
        cur = end + timedelta(days=1)

async def fetch_upstox(client, url, tokens):
    last_error = None
    for retry in range(max(8, len(tokens) * 3)):
        token = next_token(tokens)
        account_id = token['account_id']
        await throttle(account_id)
        try:
            response = await client.get(url, headers={'Authorization': f'Bearer {token["token"]}', 'Accept': 'application/json'})
        except httpx.RequestError as exc:
            last_error = exc.__class__.__name__
            await asyncio.sleep(min(2 ** retry, 10))
            continue
        if response.status_code in (429, 500, 502, 503, 504):
            last_error = f'HTTP {response.status_code}'
            if response.status_code == 429:
                retry_after = response.headers.get('Retry-After', '')
                delay = min(60, max(2, int(retry_after))) if retry_after.isdigit() else 20
                rate_cooldowns[account_id] = time.monotonic() + delay
            else: await asyncio.sleep(min(2 ** retry, 10))
            continue
        if response.status_code == 401:
            token['invalid'] = True
            if token['id'] != 'environment':
                with db() as conn: conn.execute("UPDATE api_tokens SET status='expired' WHERE id=%s", (token['id'],))
            last_error = 'access token expired'
            continue
        if response.status_code >= 400: raise RuntimeError(f'Upstox returned HTTP {response.status_code}: {response.text[:300]}')
        data = response.json()
        if data.get('status') != 'success': raise RuntimeError(str(data)[:400])
        return data['data']['candles']
    raise RuntimeError(f'Upstox retries exhausted ({last_error})')

async def upstox(client, instrument, interval, begin, end, tokens):
    unit, number, _ = INTERVALS[interval]
    encoded = quote(instrument, safe='')
    url = f'https://api.upstox.com/v3/historical-candle/{encoded}/{unit}/{number}/{end}/{begin}'
    candles = await fetch_upstox(client, url, tokens)
    if end == datetime.now(IST).date() and unit in ('minutes', 'hours'):
        try:
            intraday = await fetch_upstox(client, f'https://api.upstox.com/v3/historical-candle/intraday/{encoded}/{unit}/{number}', tokens)
            candles = list({candle[0]: candle for candle in [*candles, *intraday]}.values())
        except RuntimeError as exc:
            if not any(code in str(exc) for code in ('HTTP 400:', 'HTTP 404:')): raise
    return candles

def object_key(instrument, interval, begin, end):
    safe = instrument.replace('|', '_').replace(' ', '_')
    return f'candles/interval={interval}/instrument={safe}/year={begin.year}/window={begin}_{end}.parquet'

def write_window(instrument, interval, begin, end, candles):
    records = []
    for c in candles:
        ts = datetime.fromisoformat(c[0]).astimezone(timezone.utc)
        trading_date = ts.astimezone(IST).date()
        if begin <= trading_date <= end:
            records.append((ts, trading_date, float(c[1]), float(c[2]), float(c[3]), float(c[4]), int(c[5]), int(c[6]) if c[6] is not None else None))
    records = sorted(set(records), key=lambda x: x[0])
    key = object_key(instrument, interval, begin, end)
    if records:
        frame = pl.DataFrame(records, schema=['ts','session_date','open','high','low','close','volume','open_interest'], orient='row')
        buf = io.BytesIO()
        frame.write_parquet(buf, compression='zstd')
        path = archive_path(key)
        if path:
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_suffix('.tmp-' + uuid.uuid4().hex)
            temporary.write_bytes(buf.getvalue())
            temporary.replace(path)
        else:
            storage().put_object(Bucket=bucket(), Key=key, Body=buf.getvalue())
            (Path(os.environ.get('CACHE_DIR', '/tmp/backtest-parquet-cache')) / (hashlib.sha256(key.encode()).hexdigest() + '.parquet')).unlink(missing_ok=True)
    with db() as conn:
        conn.execute('INSERT INTO coverage (instrument,interval,from_date,to_date,rows,object_key,first_candle,last_candle) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (instrument,interval,from_date,to_date) DO UPDATE SET rows=excluded.rows, object_key=excluded.object_key, first_candle=excluded.first_candle, last_candle=excluded.last_candle, updated_at=now()', (instrument,interval,begin,end,len(records),key,min((row[1] for row in records),default=None),max((row[1] for row in records),default=None)))
    return len(records)

async def ingest(job_id, payload):
    instruments = payload['instruments']; intervals = payload['intervals']
    begin = date.fromisoformat(payload['from_date']); end = date.fromisoformat(payload['to_date'])
    tasks = [(key, period, a, b) for key in instruments for period in intervals for a,b in windows(begin,end,INTERVALS[period][2])]
    update(job_id, total=len(tasks))
    tokens = available_tokens()
    if not tokens: raise RuntimeError('No valid Upstox access tokens. Add fresh tokens in API Keys (tokens expire at 3:30 AM IST).')
    count = 0; skipped = 0; completed = 0; next_index = 0
    progress_lock = asyncio.Lock()
    async with httpx.AsyncClient(timeout=40, limits=httpx.Limits(max_connections=20)) as client:
        async def runner():
            nonlocal count, skipped, completed, next_index
            while next_index < len(tasks):
                key, period, a, b = tasks[next_index]
                next_index += 1
                try:
                    def already_stored():
                        with db() as conn:
                            return conn.execute('SELECT 1 FROM coverage WHERE instrument=%s AND interval=%s AND from_date=%s AND to_date=%s', (key,period,a,b)).fetchone() is not None
                    if await asyncio.to_thread(already_stored) and not payload.get('refresh_existing'):
                        added = 0; was_skipped = True
                    else:
                        candles = await upstox(client,key,period,a,b,tokens)
                        added = await asyncio.to_thread(write_window,key,period,a,b,candles)
                        was_skipped = False
                except Exception as exc:
                    raise RuntimeError(f'Import failed for {key} {period} {a} to {b}: {str(exc) or exc.__class__.__name__}') from exc
                async with progress_lock:
                    count += added; skipped += int(was_skipped); completed += 1
                    await asyncio.to_thread(update,job_id,progress=completed)
        try:
            async with asyncio.TaskGroup() as group:
                for _ in range(min(len(tasks), min(16, max(2, int(os.environ.get('IMPORT_WORKERS', '8')))))):
                    group.create_task(runner())
        except* Exception as group:
            raise RuntimeError(str(group.exceptions[0])) from group
    return {'candles_added':count, 'windows_skipped':skipped, 'windows_total':len(tasks)}

def load_candles(instrument, interval, begin, end):
    with db() as conn:
        rows = conn.execute('SELECT object_key FROM coverage WHERE instrument=%s AND interval=%s AND to_date >= %s AND from_date <= %s AND rows > 0 ORDER BY from_date', (instrument,interval,begin,end)).fetchall()
    frames = []
    for (key,) in rows:
        frames.append(read_archive_frame(key))
    if not frames: return None
    return pl.concat(frames).filter(pl.col('session_date').is_between(begin,end)).unique(subset=['ts'], keep='last').sort('ts')

def read_archive_frame(key):
    cache_root = Path(os.environ.get('CACHE_DIR', '/tmp/backtest-parquet-cache'))
    cache_root.mkdir(parents=True, exist_ok=True)
    local = archive_path(key)
    cached = local or cache_root / (hashlib.sha256(key.encode()).hexdigest() + '.parquet')
    if local and not local.exists(): raise FileNotFoundError(f'Archive missing: {key}')
    if not cached.exists():
        obj = storage().get_object(Bucket=bucket(), Key=key)
        temporary = cached.with_suffix('.tmp-' + uuid.uuid4().hex)
        temporary.write_bytes(obj['Body'].read())
        temporary.replace(cached)
    return pl.read_parquet(cached)

class DeleteArchive(BaseModel):
    instrument: str

@app.post('/archive/delete', dependencies=[Depends(auth)])
def delete_archive(body: DeleteArchive):
    if not re.fullmatch(r'[A-Z0-9_]+\|[A-Za-z0-9 ._-]+', body.instrument): raise HTTPException(400, 'Select an archived instrument')
    with db() as conn:
        active = conn.execute("SELECT 1 FROM jobs WHERE status IN ('queued','running') AND payload->'instruments' ? %s LIMIT 1", (body.instrument,)).fetchone()
        if active: raise HTTPException(409, 'A job is using this symbol. Wait until it finishes before deleting.')
        keys = [row[0] for row in conn.execute('SELECT object_key FROM coverage WHERE instrument=%s', (body.instrument,)).fetchall()]
        if not keys: raise HTTPException(404, 'No archive found for this instrument')
        with_files = [key for key in keys if archive_path(key) is not None]
        for key in with_files: archive_path(key).unlink(missing_ok=True)
        remote = [key for key in keys if key not in with_files]
        for i in range(0, len(remote), 1000):
            response = storage().delete_objects(Bucket=bucket(), Delete={'Objects': [{'Key': key} for key in remote[i:i+1000]], 'Quiet': True})
            if response.get('Errors'): raise HTTPException(502, 'Storage could not delete every archive file. Retry deletion.')
        cache_root = Path(os.environ.get('CACHE_DIR', '/tmp/backtest-parquet-cache'))
        for key in keys:
            (cache_root / (hashlib.sha256(key.encode()).hexdigest() + '.parquet')).unlink(missing_ok=True)
        conn.execute('DELETE FROM coverage WHERE instrument=%s', (body.instrument,))
        conn.execute('DELETE FROM instrument_names WHERE instrument=%s', (body.instrument,))
    return {'deleted_windows': len(keys), 'instrument': body.instrument}

def validate_candle_query(instrument, interval, from_date, to_date, max_days=None):
    if interval not in INTERVALS: raise HTTPException(400, 'Unknown interval')
    if not re.fullmatch(r'[A-Z0-9_]+\|[A-Za-z0-9 ._-]+', instrument): raise HTTPException(400, 'Select an archived instrument')
    if from_date > to_date: raise HTTPException(400, 'FROM must be on or before TO')
    if max_days is not None and (to_date - from_date).days > max_days: raise HTTPException(400, f'CSV export supports at most {max_days + 1} days at a time')

@app.get('/candles', dependencies=[Depends(auth)])
def candles(instrument: str, interval: str, from_date: date, to_date: date,
            cursor: str | None = None, limit: int = Query(100, ge=1, le=250)):
    validate_candle_query(instrument, interval, from_date, to_date)
    cutoff = None
    if cursor:
        try: cutoff = datetime.fromisoformat(cursor)
        except ValueError as exc: raise HTTPException(400, 'Invalid page cursor') from exc
        if cutoff.tzinfo is None: raise HTTPException(400, 'Invalid page cursor')
    with db() as conn:
        keys = conn.execute('SELECT object_key,to_date FROM coverage WHERE instrument=%s AND interval=%s AND to_date>=%s AND from_date<=%s AND rows>0 ORDER BY to_date DESC, from_date DESC', (instrument,interval,from_date,to_date)).fetchall()
    found = []; seen = set(); heap = []; index = 0
    def load_next():
        nonlocal index
        key, _ = keys[index]; index += 1
        frame = read_archive_frame(key).filter(pl.col('session_date').is_between(from_date,to_date))
        if cutoff: frame = frame.filter(pl.col('ts') < cutoff)
        iterator = iter(frame.sort('ts', descending=True).iter_rows(named=True))
        row = next(iterator, None)
        if row: heapq.heappush(heap, (-int(row['ts'].timestamp()*1_000_000), index, row, iterator))
    while heap or index < len(keys):
        if not heap:
            load_next()
            continue
        next_date = heap[0][2]['ts'].astimezone(IST).date()
        if index < len(keys) and keys[index][1] >= next_date:
            load_next()
            continue
        _, position, row, iterator = heapq.heappop(heap)
        following = next(iterator, None)
        if following: heapq.heappush(heap, (-int(following['ts'].timestamp()*1_000_000), position, following, iterator))
        if row['ts'] in seen: continue
        seen.add(row['ts']); found.append(row)
        if len(found) > limit: break
    has_more = len(found) > limit
    rows = found[:limit]
    return {'rows': [{**row, 'ts': row['ts'].astimezone(IST).isoformat(), 'session_date': row['session_date'].isoformat()} for row in rows],
            'has_more': has_more, 'next_cursor': rows[-1]['ts'].isoformat() if has_more else None, 'limit': limit}

@app.get('/candles/export', dependencies=[Depends(auth)])
def export_candles(instrument: str, interval: str, from_date: date, to_date: date):
    validate_candle_query(instrument, interval, from_date, to_date, max_days=30)
    frame = load_candles(instrument, interval, from_date, to_date)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['timestamp_ist', 'open', 'high', 'low', 'close', 'volume', 'open_interest'])
    if frame is not None:
        for row in frame.iter_rows(named=True):
            writer.writerow([row['ts'].astimezone(IST).isoformat(), row['open'], row['high'], row['low'], row['close'], row['volume'], row['open_interest']])
    output.seek(0)
    return StreamingResponse(iter([output.getvalue()]), media_type='text/csv')

def simulate(frame, instrument, config):
    """Signals on completed bar, enter next bar open, conservative stop-first ambiguous fills."""
    if frame is None or frame.height < 3: return []
    strategy = config['strategy']; rows = frame.to_dicts()
    closes = [r['close'] for r in rows]
    fast = int(config['fast']); slow = int(config['slow'])
    trades = []; active = None; pending = False; last_dip_session = None
    for i, bar in enumerate(rows):
        if pending and active is None:
            price = bar['open']; quantity = int(config['capital_per_trade'] // price) if price > 0 else 0
            if quantity:
                active = {'instrument':instrument,'entry_at':bar['ts'].isoformat(),'entry':price,'quantity':quantity}
            pending = False
        if active:
            entry = active['entry']; stop = entry * (1-config['stop_pct']/100); target = entry * (1+config['target_pct']/100)
            # Gap through a stop/target executes at bar open; both touched in a bar resolves at stop.
            if bar['open'] <= stop: exit_price, reason = bar['open'], 'stop_gap'
            elif bar['open'] >= target: exit_price, reason = bar['open'], 'target_gap'
            elif bar['low'] <= stop: exit_price, reason = stop, 'stop'
            elif bar['high'] >= target: exit_price, reason = target, 'target'
            else: exit_price, reason = None, None
            if exit_price is None and i == len(rows)-1: exit_price = bar['close']; reason='end_of_data'
            if exit_price is not None:
                active.update({'exit_at':bar['ts'].isoformat(),'exit':exit_price,'reason':reason,'pnl':round((exit_price-entry)*active['quantity'],2),'return_pct':round((exit_price/entry-1)*100,4)})
                trades.append(active); active = None
                continue
        if active is not None or i >= len(rows)-1: continue
        signal = False
        if strategy == 'dip_buy':
            # Previous session close is known before this bar. One signal per instrument per session.
            current_day = bar['session_date']
            prev_close = next((rows[j]['close'] for j in range(i-1,-1,-1) if rows[j]['session_date'] < current_day), None)
            signal = prev_close is not None and current_day != last_dip_session and bar['low'] <= prev_close*(1-config['dip_pct']/100) and rows[i+1]['session_date']==current_day
            if signal: last_dip_session = current_day
        else:
            if i >= slow:
                f0=sum(closes[i-fast+1:i+1])/fast; s0=sum(closes[i-slow+1:i+1])/slow
                f1=sum(closes[i-fast:i])/fast; s1=sum(closes[i-slow:i])/slow
                signal=f1 <= s1 and f0 > s0
        if signal: pending = True
    return trades

def run_backtest(job_id, payload):
    first=date.fromisoformat(payload['from_date']); last=date.fromisoformat(payload['to_date'])
    trades=[]; missing=[]; update(job_id,total=len(payload['instruments']))
    for i,key in enumerate(payload['instruments']):
        frame=load_candles(key,payload['interval'],first,last)
        if frame is None: missing.append(key)
        else: trades.extend(simulate(frame,key,payload))
        update(job_id,progress=i+1)
    wins=[t for t in trades if t['pnl']>0]
    return {'summary':{'trades':len(trades),'wins':len(wins),'win_rate':round(100*len(wins)/len(trades),2) if trades else 0,'net_pnl':round(sum(t['pnl'] for t in trades),2),'largest_win':max((t['pnl'] for t in trades),default=0),'largest_loss':min((t['pnl'] for t in trades),default=0)},'missing_instruments':missing,'trades':trades}

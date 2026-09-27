"""Persistent Upstox candle importer and bar-based backtest API."""
import asyncio
import io
import hashlib
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
from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel, Field, field_validator

IST = ZoneInfo('Asia/Kolkata')
INTERVALS = {'1m': ('minutes', 1, 28), '5m': ('minutes', 5, 28), '15m': ('minutes', 15, 28), '30m': ('minutes', 30, 85), '1h': ('hours', 1, 85), '1d': ('days', 1, 3000), '1w': ('weeks', 1, 8000), '1mo': ('months', 1, 8000)}
START = date(2022, 1, 1)
app = FastAPI(title='Backtest Desk worker')
queue: asyncio.Queue[str] = asyncio.Queue()
limiter = asyncio.Lock()
last_requests: list[float] = []
halfhour_requests: list[float] = []

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

async def resolve_instruments(values: list[str]):
    resolved = []
    token = os.environ.get('UPSTOX_ACCESS_TOKEN')
    async with httpx.AsyncClient(timeout=20) as client:
        for value in values:
            if '|' in value:
                resolved.append(value)
                continue
            if not token: raise HTTPException(400, 'UPSTOX_ACCESS_TOKEN is missing on the worker')
            symbol = value.upper()
            try:
                response = await client.get('https://api.upstox.com/v2/instruments/search',
                    params={'query': symbol, 'exchanges': 'NSE', 'segments': 'EQ', 'records': 30},
                    headers={'Authorization': f'Bearer {token}', 'Accept': 'application/json'})
            except httpx.RequestError as exc:
                raise HTTPException(502, f'Upstox instrument lookup failed for {symbol}: {exc.__class__.__name__}') from exc
            if response.status_code == 401: raise HTTPException(400, 'Upstox access token expired or invalid. Update UPSTOX_ACCESS_TOKEN on Railway.')
            if response.status_code != 200: raise HTTPException(502, f'Upstox instrument lookup failed for {symbol} (HTTP {response.status_code})')
            matches = [item['instrument_key'] for item in response.json().get('data', [])
                if item.get('segment') == 'NSE_EQ' and item.get('trading_symbol', '').upper() == symbol and item.get('instrument_key')]
            matches = list(dict.fromkeys(matches))
            if not matches: raise HTTPException(400, f'No exact NSE equity ticker found for {symbol}. Enter an Upstox instrument key for other instruments.')
            if len(matches) > 1: raise HTTPException(400, f'Multiple NSE equities match {symbol}. Enter the specific Upstox instrument key.')
            resolved.append(matches[0])
    return list(dict.fromkeys(resolved))

class Backfill(BaseModel):
    instruments: list[str] = Field(min_length=1, max_length=1000)
    intervals: list[str] = Field(min_length=1)
    from_date: date
    to_date: date
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

@app.on_event('shutdown')
async def shutdown():
    app.state.runner.cancel()

@app.get('/status', dependencies=[Depends(auth)])
def status():
    with db() as conn:
        rows = conn.execute('SELECT interval, count(DISTINCT instrument), sum(rows) FROM coverage GROUP BY interval').fetchall()
        running = conn.execute("SELECT count(*) FROM jobs WHERE status IN ('queued','running')").fetchone()[0]
    return {'archive': [{'interval': x, 'instruments': y, 'candles': z} for x,y,z in rows], 'active_jobs': running, 'intervals': list(INTERVALS)}

@app.get('/symbols', dependencies=[Depends(auth)])
def symbols():
    with db() as conn:
        rows = conn.execute('SELECT instrument, interval, min(from_date), max(to_date), sum(rows) FROM coverage GROUP BY instrument, interval ORDER BY instrument, interval').fetchall()
    return {'symbols': [{'instrument': a, 'interval': b, 'from_date': str(c), 'to_date': str(d), 'candles': e} for a,b,c,d,e in rows]}

def submit(kind, payload):
    job_id = str(uuid.uuid4())
    with db() as conn:
        conn.execute('INSERT INTO jobs (id,kind,status,payload) VALUES (%s,%s,%s,%s::jsonb)', (job_id, kind, 'queued', json.dumps(payload)))
    queue.put_nowait(job_id)
    return {'id': job_id, 'status': 'queued'}

@app.post('/backfill', dependencies=[Depends(auth)])
async def backfill(body: Backfill):
    if body.from_date > body.to_date or body.to_date > datetime.now(IST).date(): raise HTTPException(400, 'Invalid date range')
    body.instruments = await resolve_instruments(body.instruments)
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
            update(job_id, status='failed', error=str(exc)[:1000])
        finally:
            queue.task_done()

async def throttle():
    async with limiter:
        limit = min(int(os.environ.get('MAX_REQUESTS_PER_MINUTE', '60')), 480)
        while True:
            now = time.monotonic()
            last_requests[:] = [v for v in last_requests if now - v < 60]
            halfhour_requests[:] = [v for v in halfhour_requests if now - v < 1800]
            if len(last_requests) < limit and len(halfhour_requests) < 1900:
                last_requests.append(now)
                halfhour_requests.append(now)
                return
            wait_minute = 60 - (now - last_requests[0]) if len(last_requests) >= limit else 0
            wait_halfhour = 1800 - (now - halfhour_requests[0]) if len(halfhour_requests) >= 1900 else 0
            await asyncio.sleep(max(0.05, wait_minute, wait_halfhour))

def windows(first: date, last: date, span: int):
    cur = first
    while cur <= last:
        end = min(last, cur + timedelta(days=span - 1))
        yield cur, end
        cur = end + timedelta(days=1)

async def upstox(client, instrument, interval, begin, end):
    unit, number, _ = INTERVALS[interval]
    url = f'https://api.upstox.com/v3/historical-candle/{quote(instrument, safe="")}/{unit}/{number}/{end}/{begin}'
    for retry in range(6):
        await throttle()
        response = await client.get(url, headers={'Authorization': f'Bearer {os.environ.get("UPSTOX_ACCESS_TOKEN", "")}', 'Accept': 'application/json'})
        if response.status_code in (429, 500, 502, 503, 504):
            await asyncio.sleep(min(2 ** retry, 30)); continue
        response.raise_for_status()
        data = response.json()
        if data.get('status') != 'success': raise RuntimeError(str(data)[:400])
        return data['data']['candles']
    raise RuntimeError(f'Upstox retries exhausted: {instrument} {interval} {begin}..{end}')

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
    with db() as conn:
        conn.execute('INSERT INTO coverage (instrument,interval,from_date,to_date,rows,object_key) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (instrument,interval,from_date,to_date) DO UPDATE SET rows=excluded.rows, object_key=excluded.object_key, updated_at=now()', (instrument,interval,begin,end,len(records),key))
    return len(records)

async def ingest(job_id, payload):
    instruments = payload['instruments']; intervals = payload['intervals']
    begin = date.fromisoformat(payload['from_date']); end = date.fromisoformat(payload['to_date'])
    tasks = [(key, period, a, b) for key in instruments for period in intervals for a,b in windows(begin,end,INTERVALS[period][2])]
    update(job_id, total=len(tasks))
    count = 0; skipped = 0; completed = 0
    async with httpx.AsyncClient(timeout=30) as client:
        for key, period, a, b in tasks:
            with db() as conn:
                exists = conn.execute('SELECT rows FROM coverage WHERE instrument=%s AND interval=%s AND from_date=%s AND to_date=%s', (key,period,a,b)).fetchone()
            if exists is not None: skipped += 1
            else: count += await asyncio.to_thread(write_window, key, period, a, b, await upstox(client,key,period,a,b))
            completed += 1
            update(job_id, progress=completed)
    return {'candles_added':count, 'windows_skipped':skipped, 'windows_total':len(tasks)}

def load_candles(instrument, interval, begin, end):
    with db() as conn:
        rows = conn.execute('SELECT object_key FROM coverage WHERE instrument=%s AND interval=%s AND to_date >= %s AND from_date <= %s AND rows > 0 ORDER BY from_date', (instrument,interval,begin,end)).fetchall()
    frames = []
    cache_root = Path(os.environ.get('CACHE_DIR', '/tmp/backtest-parquet-cache'))
    cache_root.mkdir(parents=True, exist_ok=True)
    for (key,) in rows:
        local = archive_path(key)
        cached = local or cache_root / (hashlib.sha256(key.encode()).hexdigest() + '.parquet')
        if local and not local.exists(): raise FileNotFoundError(f'Archive missing: {key}')
        if not cached.exists():
            obj = storage().get_object(Bucket=bucket(), Key=key)
            temporary = cached.with_suffix('.tmp-' + uuid.uuid4().hex)
            temporary.write_bytes(obj['Body'].read())
            temporary.replace(cached)
        frames.append(pl.read_parquet(cached))
    if not frames: return None
    return pl.concat(frames).filter(pl.col('session_date').is_between(begin,end)).unique(subset=['ts'], keep='last').sort('ts')

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

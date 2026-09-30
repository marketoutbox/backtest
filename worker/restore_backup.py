"""Verify a downloaded data backup; restore it to an empty archive with --restore."""
import argparse
from contextlib import contextmanager
from datetime import date
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import uuid
import zipfile

CHUNK_SIZE = 2 * 1024 * 1024
COLUMNS = ('instrument', 'interval', 'from_date', 'to_date', 'rows', 'object_key', 'first_candle', 'last_candle')
INTERVALS = {'1m', '5m', '15m', '30m', '1h', '1d', '1w', '1mo'}


def backup_file(root, relative):
    path = PurePosixPath(relative)
    if path.is_absolute() or '..' in path.parts or '\\' in relative or not relative.startswith('files/'):
        raise ValueError('Unsafe backup file path')
    resolved = (root / relative).resolve()
    if not resolved.is_relative_to(root.resolve()):
        raise ValueError('Backup file escapes its folder')
    return resolved


@contextmanager
def open_backup_file(root, relative):
    root = Path(root)
    if root.is_file():
        path = PurePosixPath(relative)
        if path.is_absolute() or '..' in path.parts or chr(92) in relative or not relative.startswith('files/'):
            raise ValueError('Unsafe backup file path')
        with zipfile.ZipFile(root) as archive:
            name = 'backtest-data-backup/' + relative
            info = archive.getinfo(name)
            with archive.open(info) as stream:
                yield stream, info.file_size
    else:
        path = backup_file(root, relative)
        with path.open('rb') as stream:
            yield stream, path.stat().st_size


def read_manifest(root):
    root = Path(root)
    if root.is_file():
        with zipfile.ZipFile(root) as archive:
            info = archive.getinfo('backtest-data-backup/manifest.json')
            if info.file_size > 256 * 1024 * 1024:
                raise ValueError('Backup manifest is too large')
            return json.loads(archive.read(info))
    return json.loads((root / 'manifest.json').read_text())


def verify_backup(root):
    root = Path(root)
    manifest = read_manifest(root)
    if manifest.get('format') != 'backtest-desk-archive' or manifest.get('version') != 1 or manifest.get('chunk_size') != CHUNK_SIZE:
        raise ValueError('Unsupported or incomplete backup')
    coverage = manifest['coverage']
    keys = set(); identities = set(); expected_files = set()
    for row in coverage:
        if set(row) != set(COLUMNS):
            raise ValueError('Invalid coverage record')
        if row['interval'] not in INTERVALS or not isinstance(row['rows'], int) or row['rows'] < 0:
            raise ValueError('Invalid coverage interval or row count')
        if not re.fullmatch(r'[A-Z0-9_]+\|[A-Za-z0-9 ._-]+', row['instrument']):
            raise ValueError('Invalid instrument')
        start, end = date.fromisoformat(row['from_date']), date.fromisoformat(row['to_date'])
        if start > end:
            raise ValueError('Invalid coverage dates')
        for field in ('first_candle', 'last_candle'):
            if row[field] is not None and not start <= date.fromisoformat(row[field]) <= end:
                raise ValueError('Invalid candle dates')
        key = row['object_key']
        # Keys from an earlier restore may have a restored/<uuid>/ prefix.
        if not isinstance(key, str) or not key or key.startswith('/') or '..' in PurePosixPath(key).parts or '\\' in key:
            raise ValueError('Unsafe archive key')
        identity = tuple(row[c] for c in COLUMNS[:4])
        if identity in identities or key in keys:
            raise ValueError('Duplicate coverage record')
        identities.add(identity); keys.add(key)
        if row['rows']:
            expected_files.add(key)
    files = manifest['files']
    if len(files) != len(expected_files) or {f['key'] for f in files} != expected_files:
        raise ValueError('Backup inventory does not match coverage')
    total = 0
    for entry in files:
        size = entry['size']
        if not isinstance(size, int) or size <= 0:
            raise ValueError('Invalid backup size')
        if len(entry['chunks']) != (size + CHUNK_SIZE - 1) // CHUNK_SIZE:
            raise ValueError('Incomplete file checksums')
        with open_backup_file(root, entry['path']) as (source, actual_size):
            if actual_size != size:
                raise ValueError(f'Backup file size mismatch: {entry["path"]}')
            for expected in entry['chunks']:
                if hashlib.sha256(source.read(CHUNK_SIZE)).hexdigest() != expected:
                    raise ValueError(f'Backup checksum mismatch: {entry["path"]}')
        total += size
    seen = set()
    for row in manifest['instrument_names']:
        if set(row) != {'instrument', 'symbol'} or not all(isinstance(v, str) for v in row.values()) or row['instrument'] in seen:
            raise ValueError('Invalid instrument labels')
        seen.add(row['instrument'])
    return manifest, total


def restore(root, manifest):
    # Dependencies and environment are only required for an actual restore.
    import boto3
    from boto3.s3.transfer import TransferConfig
    import psycopg
    from main import SCHEMA

    url = os.environ.get('DATABASE_URL')
    archive = os.environ.get('ARCHIVE_DIR')
    bucket = os.environ.get('S3_BUCKET')
    if not url or (not archive and not bucket):
        raise ValueError('Set DATABASE_URL and either ARCHIVE_DIR or the S3 storage variables')
    client = None if archive else boto3.client('s3', endpoint_url=os.environ.get('S3_ENDPOINT_URL') or None,
        aws_access_key_id=os.environ.get('S3_ACCESS_KEY_ID'), aws_secret_access_key=os.environ.get('S3_SECRET_ACCESS_KEY'),
        region_name=os.environ.get('S3_REGION') or 'auto')
    prefix = 'restored/' + uuid.uuid4().hex + '/'
    with psycopg.connect(url) as conn:
        for statement in SCHEMA.split(';'):
            if statement.strip(): conn.execute(statement)
    # Keep archive/job mutations out until all uploads and metadata are committed.
    # Run with the worker stopped; these table locks also prevent accidental writes.
    with psycopg.connect(url) as conn:
        conn.execute("SET lock_timeout = '10s'")
        conn.execute('LOCK TABLE jobs,coverage,instrument_names IN ACCESS EXCLUSIVE MODE')
        if conn.execute('SELECT 1 FROM coverage LIMIT 1').fetchone():
            raise ValueError('Restore requires an empty archive database. Use a new database to protect existing data.')
        if conn.execute("SELECT 1 FROM jobs WHERE status IN ('queued','running') LIMIT 1").fetchone():
            raise ValueError('The target has active jobs; stop them before restoring')
        for index, entry in enumerate(manifest['files']):
            # New independent keys avoid overwriting files from any existing archive.
            key = prefix + hashlib.sha256(entry['key'].encode()).hexdigest() + '.parquet'
            if archive:
                target = Path(archive) / key
                target.parent.mkdir(parents=True, exist_ok=True)
                temporary = target.with_suffix('.tmp')
                try:
                    with open_backup_file(root, entry['path']) as (reader, _), temporary.open('wb') as writer:
                        for expected in entry['chunks']:
                            data = reader.read(CHUNK_SIZE)
                            if hashlib.sha256(data).hexdigest() != expected:
                                raise ValueError('Backup changed after verification')
                            writer.write(data)
                    temporary.replace(target)
                finally:
                    temporary.unlink(missing_ok=True)
            else:
                with open_backup_file(root, entry['path']) as (reader, _):
                    client.upload_fileobj(reader, bucket, key, Config=TransferConfig(use_threads=False, multipart_chunksize=8*1024*1024))
            print(f'Uploaded {index + 1}/{len(manifest["files"])} files', flush=True)
        for row in manifest['coverage']:
            copied = dict(row)
            copied['object_key'] = prefix + hashlib.sha256(row['object_key'].encode()).hexdigest() + '.parquet'
            conn.execute('INSERT INTO coverage (' + ','.join(COLUMNS) + ') VALUES (' + ','.join(['%s'] * len(COLUMNS)) + ')', tuple(copied[c] for c in COLUMNS))
        for row in manifest['instrument_names']:
            conn.execute('INSERT INTO instrument_names (instrument,symbol) VALUES (%s,%s) ON CONFLICT (instrument) DO UPDATE SET symbol=excluded.symbol', (row['instrument'], row['symbol']))
    print('Restore complete. Start the worker and check Data Viewer. Re-add your API tokens for future imports.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('folder', type=Path, help='Downloaded backup ZIP (or legacy backtest-data-backup folder)')
    parser.add_argument('--restore', action='store_true', help='Upload verified data to an EMPTY target archive')
    args = parser.parse_args()
    manifest, size = verify_backup(args.folder)
    print(f'Verified {len(manifest["files"])} files, {size / 1024**3:.2f} GiB, {len(manifest["coverage"])} coverage windows.')
    if args.restore:
        restore(args.folder, manifest)
    else:
        print('Verification only. No database or storage changes made. Add --restore to restore to the configured empty target.')


if __name__ == '__main__':
    main()

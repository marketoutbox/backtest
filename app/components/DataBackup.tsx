'use client';

import { useEffect, useRef, useState } from 'react';

type Folder = {getDirectoryHandle: (name: string, options?: {create?: boolean}) => Promise<Folder>;
  getFileHandle: (name: string, options?: {create?: boolean}) => Promise<FileSystemFileHandle>;
  removeEntry: (name: string) => Promise<void>};
type WindowRow = {instrument: string; interval: string; from_date: string; to_date: string;
  rows: number; object_key: string; first_candle: string | null; last_candle: string | null};
type SavedFile = {key: string; path: string; size: number; version: string; chunks: string[]};
const hex = async (data: BufferSource) => Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256', data)), n => n.toString(16).padStart(2, '0')).join('');

async function writeJson(folder: Folder, name: string, value: unknown) {
  const handle = await folder.getFileHandle(name, {create: true});
  const stream = await handle.createWritable();
  try { await stream.write(JSON.stringify(value)); await stream.close(); }
  catch (error) { await stream.abort().catch(() => {}); throw error; }
}

export default function DataBackup() {
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState('');
  const [error, setError] = useState('');
  const controller = useRef<AbortController | null>(null);

  useEffect(() => () => controller.current?.abort(), []);
  useEffect(() => {
    if (!busy) return;
    const warn = (event: BeforeUnloadEvent) => { event.preventDefault(); event.returnValue = ''; };
    window.addEventListener('beforeunload', warn);
    return () => window.removeEventListener('beforeunload', warn);
  }, [busy]);

  async function download() {
    const picker = (window as unknown as {showDirectoryPicker?: (options: {mode: string}) => Promise<Folder>}).showDirectoryPicker;
    if (!picker) { setError('Use Chrome or Edge on HTTPS to save a large backup directly to your PC.'); return; }
    let folder: Folder;
    try { folder = await picker.call(window, {mode: 'readwrite'}); }
    catch (e) { if ((e as Error).name !== 'AbortError') setError((e as Error).message); return; }
    const abort = new AbortController(); controller.current = abort;
    setBusy(true); setError(''); setMessage('Preparing archive backup…');
    let sessionId = '';
    const request = async (path: string, method = 'GET') => {
      const response = await fetch(`/api/backup${path}`, {method, cache: 'no-store', signal: abort.signal});
      if (!response.ok) {
        const text = await response.text(); let detail = `Backup request failed (${response.status})`;
        try { detail = JSON.parse(text).detail || detail; } catch { /* non-JSON gateway error */ }
        throw new Error(detail);
      }
      return response;
    };
    try {
      const session = await (await request('', 'POST')).json(); sessionId = session.id;
      const root = await folder.getDirectoryHandle('backtest-data-backup', {create: true});
      const filesFolder = await root.getDirectoryHandle('files', {create: true});
      const receipts = await root.getDirectoryHandle('receipts', {create: true});
      // Only a fully completed download has a restore manifest.
      await root.removeEntry('manifest.json').catch(e => { if (e.name !== 'NotFoundError') throw e; });
      const coverage: WindowRow[] = []; const names: {instrument: string; symbol: string}[] = [];
      for (let offset = 0; offset < Math.max(session.windows, session.name_count); offset += session.page_size) {
        const page = await (await request(`/${sessionId}/manifest?offset=${offset}`)).json();
        coverage.push(...page.coverage); names.push(...page.instrument_names);
      }
      const files: SavedFile[] = []; let transferred = 0; let completed = 0;
      for (let index = 0; index < coverage.length; index++) {
        if (!coverage[index].rows) continue;
        abort.signal.throwIfAborted();
        const key = coverage[index].object_key;
        const filename = `${await hex(new TextEncoder().encode(key))}.parquet`;
        let response = await request(`/${sessionId}/files/${index}?offset=0`);
        const size = Number(response.headers.get('X-Backup-Size'));
        const version = response.headers.get('X-Backup-Version') || '';
        if (!Number.isSafeInteger(size) || size <= 0 || !version) throw new Error('Invalid backup file metadata');
        let firstChunk = await response.arrayBuffer();
        if (await hex(firstChunk) !== response.headers.get('X-Backup-SHA256')) throw new Error('Download checksum mismatch. Retry the backup.');
        let saved: SavedFile | null = null;
        try {
          const receipt = JSON.parse(await (await (await receipts.getFileHandle(filename + '.json')).getFile()).text()) as SavedFile;
          if (receipt.key === key && receipt.version === version && receipt.size === size && receipt.path === `files/${filename}` && receipt.chunks.length === Math.ceil(size / session.chunk_size)) {
            const local = await (await filesFolder.getFileHandle(filename)).getFile();
            let valid = local.size === size;
            for (let part = 0; valid && part < receipt.chunks.length; part++) {
              abort.signal.throwIfAborted();
              valid = await hex(await local.slice(part * session.chunk_size, Math.min(size, (part + 1) * session.chunk_size)).arrayBuffer()) === receipt.chunks[part];
            }
            if (valid) saved = receipt;
          }
        } catch (e) { if (abort.signal.aborted) throw e; /* redownload missing or damaged files */ }
        if (!saved) {
          const output = await (await filesFolder.getFileHandle(filename, {create: true})).createWritable();
          const hashes: string[] = [];
          try {
            for (let offset = 0; offset < size; offset += session.chunk_size) {
              abort.signal.throwIfAborted();
              if (offset) { response = await request(`/${sessionId}/files/${index}?offset=${offset}`); firstChunk = await response.arrayBuffer(); }
              if (response.headers.get('X-Backup-Version') !== version || Number(response.headers.get('X-Backup-Size')) !== size) throw new Error('An archive file changed. Retry the backup.');
              const hash = await hex(firstChunk);
              if (firstChunk.byteLength !== Math.min(session.chunk_size, size - offset) || hash !== response.headers.get('X-Backup-SHA256')) throw new Error('Download checksum mismatch. Retry the backup.');
              await output.write(firstChunk); hashes.push(hash); transferred += firstChunk.byteLength;
              setMessage(`${completed} / ${session.files} files saved · ${(transferred / 1024 ** 3).toFixed(2)} GB downloaded. Keep this tab open.`);
            }
            await output.close();
          } catch (e) { await output.abort().catch(() => {}); throw e; }
          saved = {key, path: `files/${filename}`, size, version, chunks: hashes};
          await writeJson(receipts, filename + '.json', saved);
        }
        files.push(saved); completed++;
        setMessage(`${completed} / ${session.files} files saved or verified · ${(transferred / 1024 ** 3).toFixed(2)} GB downloaded.`);
      }
      await writeJson(root, 'manifest.json', {format: session.format, version: session.version,
        created_at: session.created_at, chunk_size: session.chunk_size, coverage, instrument_names: names, files});
      setMessage(`Backup complete: ${files.length} price files and the archive index saved in backtest-data-backup. Keep the entire folder together.`);
    } catch (e) {
      setError(abort.signal.aborted ? 'Backup cancelled. Download into the same parent folder to resume completed files.' : `${(e as Error).message} Download into the same parent folder to resume.`);
      setMessage('');
    } finally {
      if (sessionId) await fetch(`/api/backup/${sessionId}`, {method: 'DELETE'}).catch(() => {});
      controller.current = null; setBusy(false);
    }
  }
  return <section className="panel backup-panel">
    <div><h2>Data backup</h2><p>Save all stocks and timeframes as compressed price files, with the archive index needed to restore them. Keep free disk space for the full archive.</p>
      <p className="footnote">Use Chrome or Edge. Finish active jobs first. Imports, updates and deletions pause during download. To resume, choose the same parent folder. This backs up price data and symbol labels; API tokens and backtest history are excluded.</p></div>
    <button className="primary" onClick={download} disabled={busy}>Download data backup</button>
    {busy && <button className="plain" onClick={() => controller.current?.abort()}>Cancel backup</button>}
    {message && <p role="status" className="footnote">{message}</p>}
    {error && <p role="alert" className="alert danger">{error}</p>}
  </section>;
}

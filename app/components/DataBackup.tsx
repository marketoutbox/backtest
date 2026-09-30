'use client';

import { useCallback, useEffect, useRef, useState } from 'react';

type Backup = {id: string; status: string; progress: number; total: number; error?: string;
  created_at: string; details?: {stage?: string; bytes?: number}; result?: {size: number; files: number}};
const size = (bytes: number) => `${(bytes / 1024 ** 3).toFixed(2)} GB`;

export default function DataBackup() {
  const [backups, setBackups] = useState<Backup[]>([]);
  const [busy, setBusy] = useState(false);
  const [loading, setLoading] = useState(true);
  const [message, setMessage] = useState('');
  const [error, setError] = useState('');
  const pending = useRef(false);
  const request = useCallback(async (path = '', method = 'GET') => {
    const response = await fetch(`/api/backup${path}`, {method, cache: 'no-store', signal: AbortSignal.timeout(60000)});
    const text = await response.text();
    let data;
    try { data = JSON.parse(text); } catch { throw new Error(`Backup service returned HTTP ${response.status}. Check that the worker is updated.`); }
    if (!response.ok) throw new Error(data.detail || `Backup request failed (${response.status})`);
    return data;
  }, []);
  const refresh = useCallback(async () => {
    try { setBackups((await request()).backups); setError(''); }
    catch (e) { setError((e as Error).message); }
    finally { setLoading(false); }
  }, [request]);
  useEffect(() => { refresh(); const timer = setInterval(refresh, 5000); return () => clearInterval(timer); }, [refresh]);
  const active = backups.some(item => ['queued', 'running'].includes(item.status));

  async function prepare() {
    if (pending.current) return;
    pending.current = true; setBusy(true); setError(''); setMessage('Starting server backup…');
    try {
      await request('', 'POST');
      setMessage('Backup started on the server. You can close this page and return later to download it.');
      await refresh();
    } catch (e) { setError((e as Error).message); setMessage(''); }
    finally { pending.current = false; setBusy(false); }
  }
  async function remove(id: string) {
    if (!window.confirm('Delete this generated backup ZIP from the server? Your stock price archive will remain available.')) return;
    setBusy(true); setError('');
    try { await request(`/${id}`, 'DELETE'); await refresh(); }
    catch (e) { setError((e as Error).message); }
    finally { setBusy(false); }
  }

  return <section className="panel backup-panel">
    <h2>Data backup</h2>
    <p>Prepare a ZIP of all stocks, timeframes and the archive index on the server. When it is ready, use the download link to save it to your PC.</p>
    <p className="footnote">Preparation continues if you close this page. Finish active jobs first. A local-volume backup needs additional free space about the size of your archive. API tokens and backtest history are excluded.</p>
    <button className="primary" onClick={prepare} disabled={busy || loading || active}>{busy ? 'Please wait…' : active ? 'Preparing backup on server…' : 'Prepare data backup'}</button>
    {message && <p role="status" className="footnote">{message}</p>}
    {error && <p role="alert" className="alert danger">{error}</p>}
    {loading && <p className="footnote">Loading server backups…</p>}
    <div className="backup-list">{backups.map(item => <div className="backup-item" key={item.id}>
      <div><strong>{new Date(item.created_at).toLocaleString()}</strong>
        {['queued', 'running'].includes(item.status) ? <p role="status">{item.status === 'queued' ? 'Queued on server' : item.details?.stage === 'finalizing' ? 'Finalizing ZIP…' : `Preparing: ${item.progress.toLocaleString()} / ${item.total.toLocaleString()} files · ${size(item.details?.bytes || 0)} processed`}</p>
          : item.status === 'complete' ? <p>Ready · {size(item.result?.size || 0)} · {item.result?.files.toLocaleString()} files</p>
          : <p role="alert">Failed: {item.error || 'Please prepare a new backup.'}</p>}
      </div>
      {item.status === 'complete' && <div className="backup-item-actions">
        <a href={`/api/backup/${encodeURIComponent(item.id)}/download`} target="_blank" rel="noreferrer">Download ZIP</a>
        <button className="plain" onClick={() => remove(item.id)} disabled={busy}>Delete server backup</button>
      </div>}
    </div>)}</div>
    <p className="footnote">Keep the downloaded ZIP for restoration. Download links are temporary; click Download ZIP again if a link expires. Delete old server backups after saving your copies to reclaim storage.</p>
  </section>;
}

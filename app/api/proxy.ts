import { NextRequest, NextResponse } from 'next/server';
export async function forward(req: NextRequest, path: string, method = 'GET') {
  const base = process.env.WORKER_URL;
  const secret = process.env.WORKER_SECRET;
  if (!base || !secret) return NextResponse.json({ detail: 'Worker is not configured. Set WORKER_URL and WORKER_SECRET.' }, { status: 503 });
  try {
    const upstream = await fetch(`${base.replace(/\/$/, '')}${path}`, { method, headers: { 'X-Worker-Secret': secret, 'Content-Type': 'application/json' }, body: method === 'GET' ? undefined : await req.text(), cache: 'no-store', signal: AbortSignal.timeout(25000) });
    const body = await upstream.text();
    return new NextResponse(body, { status: upstream.status, headers: { 'Content-Type': upstream.headers.get('content-type') || 'application/json', 'Cache-Control': 'no-store' } });
  } catch { return NextResponse.json({ detail: 'Worker is unreachable.' }, { status: 502 }); }
}

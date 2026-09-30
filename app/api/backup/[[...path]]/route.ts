import { NextRequest, NextResponse } from 'next/server';

export const dynamic = 'force-dynamic';
export const maxDuration = 60;

async function proxy(req: NextRequest, context: {params: Promise<{path?: string[]}>}) {
  const base = process.env.WORKER_URL;
  const secret = process.env.WORKER_SECRET;
  if (!base || !secret) return NextResponse.json({detail: 'Worker is not configured.'}, {status: 503});
  const {path = []} = await context.params;
  const suffix = path.map(encodeURIComponent).join('/');
  try {
    const response = await fetch(`${base.replace(/\/$/, '')}/backup${suffix ? '/' + suffix : ''}${req.nextUrl.search}`, {
      method: req.method, headers: {'X-Worker-Secret': secret}, cache: 'no-store',
      signal: AbortSignal.timeout(55000),
    });
    const headers = new Headers({'Cache-Control': 'no-store'});
    for (const name of ['Content-Type', 'X-Backup-Size', 'X-Backup-Version', 'X-Backup-SHA256']) {
      const value = response.headers.get(name);
      if (value) headers.set(name, value);
    }
    return new Response(response.body, {status: response.status, headers});
  } catch {
    return NextResponse.json({detail: 'Backup request interrupted. Retry the download into the same folder.'}, {status: 502});
  }
}
export const GET = proxy;
export const POST = proxy;
export const DELETE = proxy;

import { NextRequest, NextResponse } from 'next/server';

export const dynamic = 'force-dynamic';
export const maxDuration = 60;

async function proxy(req: NextRequest, context: {params: Promise<{path?: string[]}>}) {
  const base = process.env.WORKER_URL;
  const secret = process.env.WORKER_SECRET;
  if (!base || !secret) return NextResponse.json({detail: 'Worker is not configured.'}, {status: 503});
  const {path = []} = await context.params;
  const download = req.method === 'GET' && path.length === 2 && path[1] === 'download';
  const suffix = (download ? [path[0], 'link'] : path).map(encodeURIComponent).join('/');
  try {
    const response = await fetch(`${base.replace(/\/$/, '')}/backup${suffix ? '/' + suffix : ''}`, {
      method: req.method, headers: {'X-Worker-Secret': secret}, cache: 'no-store',
      signal: AbortSignal.timeout(55000),
    });
    if (download && response.ok) {
      const {url} = await response.json();
      const destination = new URL(url, base.replace(/\/$/, '') + '/');
      if (!['https:', 'http:'].includes(destination.protocol)) throw new Error('Invalid download URL');
      // Only a small signed-link response passes through Next.js. The ZIP does not.
      return new NextResponse(null, {status: 303, headers: {Location: destination.toString(), 'Cache-Control': 'private, no-store', 'Referrer-Policy': 'no-referrer'}});
    }
    return new Response(response.body, {status: response.status, headers: {
      'Content-Type': response.headers.get('content-type') || 'application/json', 'Cache-Control': 'no-store',
    }});
  } catch {
    return NextResponse.json({detail: 'Could not reach the backup service. Refresh to check whether the job started; server preparation continues independently.'}, {status: 502});
  }
}
export const GET = proxy;
export const POST = proxy;
export const DELETE = proxy;

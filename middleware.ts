import { NextRequest, NextResponse } from 'next/server';

export function middleware(req: NextRequest) {
  // Fail closed while the project is being imported. Enable Vercel Authentication
  // for All Deployments before setting SITE_ACCESS_MODE=vercel.
  if (process.env.SITE_ACCESS_MODE === 'vercel') return NextResponse.next();

  const user = process.env.SITE_USERNAME;
  const pass = process.env.SITE_PASSWORD;
  if (!user || !pass) return new NextResponse('Access is not configured', { status: 503 });
  const encoded = req.headers.get('authorization')?.match(/^Basic (.+)$/i)?.[1];
  let credentials = '';
  try { credentials = encoded ? atob(encoded) : ''; } catch { /* invalid header */ }
  if (credentials !== `${user}:${pass}`) return new NextResponse('Authentication required', { status: 401, headers: { 'WWW-Authenticate': 'Basic realm="Backtest Desk"' } });
  return NextResponse.next();
}
export const config = { matcher: ['/((?!_next/static|_next/image|favicon.ico).*)'] };

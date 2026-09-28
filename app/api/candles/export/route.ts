import { NextRequest } from 'next/server';
import { NextResponse } from 'next/server';

export const dynamic = 'force-dynamic';
export async function GET(req: NextRequest) {
  const base=process.env.WORKER_URL;const secret=process.env.WORKER_SECRET;
  if(!base||!secret)return NextResponse.json({detail:'Worker is not configured.'},{status:503});
  try{
    const upstream=await fetch(`${base.replace(/\/$/,'')}/candles/export${req.nextUrl.search}`,{
      headers:{'X-Worker-Secret':secret},cache:'no-store',signal:AbortSignal.timeout(120000)});
    return new Response(upstream.body,{status:upstream.status,headers:{'Content-Type':upstream.headers.get('content-type')||'text/csv','Cache-Control':'no-store'}});
  }catch{return NextResponse.json({detail:'Worker is unreachable during CSV export.'},{status:502})}
}

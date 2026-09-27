import { NextRequest } from 'next/server'; import { forward } from '../../proxy';
export const dynamic = 'force-dynamic'; export async function GET(req: NextRequest, { params }: {params: Promise<{id:string}>}) { const {id}=await params; return forward(req, `/jobs/${encodeURIComponent(id)}`); }

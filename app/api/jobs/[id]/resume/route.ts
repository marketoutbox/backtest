import { NextRequest } from 'next/server';
import { forward } from '../../../proxy';
export async function POST(req: NextRequest, context: { params: Promise<{id:string}> }) {
 const {id}=await context.params;
 return forward(req, `/jobs/${encodeURIComponent(id)}/resume`, 'POST');
}

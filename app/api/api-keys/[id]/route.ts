import { NextRequest } from 'next/server';
import { forward } from '../../proxy';

export async function DELETE(req: NextRequest, {params}:{params:Promise<{id:string}>}) {
  const {id}=await params;
  return forward(req, `/api-keys/${encodeURIComponent(id)}`, 'DELETE');
}

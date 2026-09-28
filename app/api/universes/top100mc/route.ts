import { NextRequest } from 'next/server';
import { forward } from '../../proxy';
export async function GET(req: NextRequest) { return forward(req, '/universes/top100mc'); }

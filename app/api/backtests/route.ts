import { NextRequest } from 'next/server'; import { forward } from '../proxy';
export async function POST(req: NextRequest) { return forward(req, '/backtests', 'POST'); }

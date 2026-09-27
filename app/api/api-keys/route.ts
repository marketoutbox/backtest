import { NextRequest } from 'next/server';
import { forward } from '../proxy';

export const dynamic = 'force-dynamic';
export async function GET(req: NextRequest) { return forward(req, '/api-keys'); }
export async function POST(req: NextRequest) { return forward(req, '/api-keys', 'POST'); }

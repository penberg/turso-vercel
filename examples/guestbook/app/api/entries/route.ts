import { createDb } from '@tursodatabase/vercel-experimental';
import { NextResponse } from 'next/server';

export async function GET() {
  const db = await createDb('guestbook');

  const result = await db.query('SELECT id, name, message, created_at FROM entries ORDER BY id DESC LIMIT 50');

  const entries = result.rows.map(row => ({
    id: row[0],
    name: row[1],
    message: row[2],
    created_at: row[3],
  }));

  return NextResponse.json(entries);
}

export async function POST(request: Request) {
  const { name, message } = await request.json();

  if (!name || !message) {
    return NextResponse.json({ error: 'Name and message are required' }, { status: 400 });
  }

  const db = await createDb('guestbook');

  await db.execute(
    'INSERT INTO entries (name, message) VALUES (?, ?)',
    [name, message]
  );
  await db.push();

  return NextResponse.json({ success: true });
}

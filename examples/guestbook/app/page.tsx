import { createDb } from '@tursodatabase/vercel-experimental';
import { revalidatePath } from 'next/cache';

interface GuestbookEntry {
  id: number;
  name: string;
  message: string;
  created_at: string;
}

async function getEntries(): Promise<GuestbookEntry[]> {
  const db = await createDb('guestbook');

  // Create table if it doesn't exist
  await db.execute(`
    CREATE TABLE IF NOT EXISTS entries (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      message TEXT NOT NULL,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
  `);

  const result = await db.query('SELECT id, name, message, created_at FROM entries ORDER BY id DESC LIMIT 50');

  return result.rows.map(row => ({
    id: row[0] as number,
    name: row[1] as string,
    message: row[2] as string,
    created_at: row[3] as string,
  }));
}

async function addEntry(formData: FormData) {
  'use server';

  const name = formData.get('name') as string;
  const message = formData.get('message') as string;

  if (!name || !message) return;

  const db = await createDb('guestbook');
  await db.execute(
    'INSERT INTO entries (name, message) VALUES (?, ?)',
    [name, message]
  );
  await db.push();

  revalidatePath('/');
}

export default async function Home() {
  const entries = await getEntries();

  return (
    <main>
      <h1>Guestbook</h1>
      <p style={{ color: '#666' }}>
        Powered by Turso + Vercel
      </p>

      <form action={addEntry} style={{ marginBottom: '2rem' }}>
        <div style={{ marginBottom: '1rem' }}>
          <label htmlFor="name" style={{ display: 'block', marginBottom: '0.5rem' }}>Name</label>
          <input
            type="text"
            id="name"
            name="name"
            required
            style={{ width: '100%', padding: '0.5rem', fontSize: '1rem' }}
          />
        </div>
        <div style={{ marginBottom: '1rem' }}>
          <label htmlFor="message" style={{ display: 'block', marginBottom: '0.5rem' }}>Message</label>
          <textarea
            id="message"
            name="message"
            required
            rows={3}
            style={{ width: '100%', padding: '0.5rem', fontSize: '1rem' }}
          />
        </div>
        <button
          type="submit"
          style={{
            backgroundColor: '#0070f3',
            color: 'white',
            border: 'none',
            padding: '0.75rem 1.5rem',
            fontSize: '1rem',
            cursor: 'pointer',
            borderRadius: '4px',
          }}
        >
          Sign Guestbook
        </button>
      </form>

      <h2>Entries ({entries.length})</h2>

      {entries.length === 0 ? (
        <p style={{ color: '#666' }}>No entries yet. Be the first to sign!</p>
      ) : (
        <ul style={{ listStyle: 'none', padding: 0 }}>
          {entries.map(entry => (
            <li
              key={entry.id}
              style={{
                borderBottom: '1px solid #eee',
                padding: '1rem 0',
              }}
            >
              <strong>{entry.name}</strong>
              <span style={{ color: '#666', marginLeft: '0.5rem', fontSize: '0.875rem' }}>
                {entry.created_at}
              </span>
              <p style={{ margin: '0.5rem 0 0 0' }}>{entry.message}</p>
            </li>
          ))}
        </ul>
      )}
    </main>
  );
}

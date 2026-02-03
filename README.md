<p align="center">
  <a href="https://turso.tech/">
    <picture>
      <img src="/.github/assets/cover.png" alt="Turso database for Vercel" />
    </picture>
  </a>
  <h1 align="center">Turso + Vercel Integration</h1>
</p>

<p align="center">
  Zero-config SQLite databases for Vercel Functions.
</p>

<p align="center">
  <a href="https://turso.tech"><strong>Turso</strong></a> ·
  <a href="https://docs.turso.tech"><strong>Docs</strong></a> ·
  <a href="https://turso.tech/blog"><strong>Blog &amp; Tutorials</strong></a>
</p>

<p align="center">
  <a href="LICENSE">
    <picture>
      <img src="https://img.shields.io/github/license/tursodatabase/turso-vercel?color=0F624B" alt="MIT License" />
    </picture>
  </a>
  <a href="https://tur.so/discord-ts">
    <picture>
      <img src="https://img.shields.io/discord/933071162680958986?color=0F624B" alt="Discord" />
    </picture>
  </a>
  <a href="https://www.npmjs.com/package/@tursodatabase/vercel-experimental">
    <picture>
      <img src="https://img.shields.io/npm/v/@tursodatabase/vercel-experimental?color=0F624B" alt="npm version" />
    </picture>
  </a>
</p>

> **⚠️ Warning:** This software is in BETA. It may still contain bugs and unexpected behavior. Use caution with production data and ensure you have backups.

## Features

- **Zero-config databases** &mdash; Databases are created automatically on first use
- **Local SQLite** &mdash; Fast reads from a local database copy in the serverless function
- **Automatic sync** &mdash; Writes are automatically pushed to Turso via Vercel's `waitUntil`
- **Partial sync** &mdash; Sync only the data you need for efficient cold starts

## Install

```bash
npm install @tursodatabase/vercel-experimental
```

## Setup

1. Get your Turso API token:
   ```bash
   turso auth api-tokens mint my-vercel-token
   ```

2. Get your organization slug:
   ```bash
   turso org list
   ```

3. Add environment variables to your Vercel project:
   ```
   TURSO_API_TOKEN=your-api-token
   TURSO_ORG=your-org-slug
   ```

## Quickstart

```ts
import { createDb } from "@tursodatabase/vercel-experimental";

// Get or create a database (automatic provisioning)
const db = await createDb("my-database");

// Create tables
await db.execute(`
  CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT UNIQUE
  )
`);

// Insert data
await db.execute(
  "INSERT INTO users (name, email) VALUES (?, ?)",
  ["Alice", "alice@example.com"]
);

// Changes sync automatically via waitUntil - no manual push needed!

// Query data
const result = await db.query("SELECT * FROM users");
console.log(result.rows);
```

## API Reference

### `createDb(name, options?)`

Creates or retrieves a database instance.

```ts
const db = await createDb("my-database", {
  group: "default",        // Database group (optional)
  partialSync: true,       // Enable partial sync (optional)
});
```

### `db.query(sql, params?)`

Execute a SELECT query and return results.

```ts
const result = await db.query(
  "SELECT * FROM users WHERE id = ?",
  [1]
);

console.log(result.columns); // ["id", "name", "email"]
console.log(result.rows);    // [[1, "Alice", "alice@example.com"]]
```

### `db.execute(sql, params?)`

Execute an INSERT, UPDATE, DELETE, or DDL statement.

```ts
await db.execute(
  "UPDATE users SET name = ? WHERE id = ?",
  ["Bob", 1]
);
```

### `db.push()`

Manually push local changes to the remote Turso database. This is optional since changes are automatically synced via Vercel's `waitUntil`, but useful when you need to ensure data is synced before responding.

```ts
await db.execute("INSERT INTO users (name) VALUES (?)", ["Charlie"]);
await db.push(); // Explicit sync (optional - auto-sync happens via waitUntil)
```

### `db.pull()`

Pull latest changes from the remote Turso database.

```ts
await db.pull(); // Get latest data from Turso
```

## Examples

### Next.js Server Component

```tsx
import { createDb } from "@tursodatabase/vercel-experimental";

async function getUsers() {
  const db = await createDb("my-app");
  const result = await db.query("SELECT * FROM users");
  return result.rows;
}

export default async function UsersPage() {
  const users = await getUsers();

  return (
    <ul>
      {users.map((user) => (
        <li key={user[0]}>{user[1]}</li>
      ))}
    </ul>
  );
}
```

### Next.js Server Action

```tsx
import { createDb } from "@tursodatabase/vercel-experimental";
import { revalidatePath } from "next/cache";

async function addUser(formData: FormData) {
  "use server";

  const name = formData.get("name") as string;
  const db = await createDb("my-app");

  await db.execute("INSERT INTO users (name) VALUES (?)", [name]);
  await db.push();

  revalidatePath("/users");
}
```

### API Route

```ts
import { createDb } from "@tursodatabase/vercel-experimental";
import { NextResponse } from "next/server";

export async function GET() {
  const db = await createDb("my-app");
  const result = await db.query("SELECT * FROM users");

  return NextResponse.json(result.rows);
}

export async function POST(request: Request) {
  const { name } = await request.json();
  const db = await createDb("my-app");

  await db.execute("INSERT INTO users (name) VALUES (?)", [name]);
  await db.push();

  return NextResponse.json({ success: true });
}
```

## Documentation

Visit our [official documentation](https://docs.turso.tech) for more details.

## Support

Join us [on Discord](https://tur.so/discord-ts) to get help using this SDK. Report security issues [via email](mailto:security@turso.tech).

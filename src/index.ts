import { join } from "node:path";
import { tmpdir } from "node:os";
import { createClient } from "@tursodatabase/api";
import { connect, type Database, type DatabaseOpts } from "@tursodatabase/sync";
import { waitUntil } from "@vercel/functions";

// ============================================================================
// Types
// ============================================================================

export interface QueryResult {
  columns: string[];
  rows: unknown[][];
}

export interface DatabaseOptions {
  partialSync?: boolean;
  bootstrapStrategy?: NonNullable<DatabaseOpts["partialSyncExperimental"]>["bootstrapStrategy"];
}

export interface CreateDbOptions extends DatabaseOptions {
  group?: string;
}

interface Credentials {
  url: string;
  authToken: string;
}

// ============================================================================
// State
// ============================================================================

const instances = new Map<string, Promise<TursoDatabase>>();
const credentials = new Map<string, Credentials>();
const pendingFlush = new Set<TursoDatabase>();

let apiClient: ReturnType<typeof createClient> | null = null;
let apiClientOrg: string | null = null;
let flushScheduled = false;

// ============================================================================
// Database Class
// ============================================================================

export class TursoDatabase {
  private db: Database;
  private dirty = false;

  private constructor(db: Database) {
    this.db = db;
  }

  static async open(
    localPath: string,
    url: string,
    authToken: string,
    options?: DatabaseOptions
  ): Promise<TursoDatabase> {
    const opts: DatabaseOpts = { path: localPath, url, authToken };

    if (options?.partialSync) {
      opts.partialSyncExperimental = {
        bootstrapStrategy: options.bootstrapStrategy ?? { kind: "prefix", length: 128 * 1024 },
      };
    }

    return new TursoDatabase(await connect(opts));
  }

  async query(sql: string, params?: unknown[]): Promise<QueryResult> {
    const stmt = this.db.prepare(sql);
    try {
      const columns = stmt.columns().map((c) => c.name);
      const rows = await stmt.all(...(params ?? []));
      return { columns, rows: rows.map((row) => Object.values(row)) };
    } finally {
      stmt.close();
    }
  }

  async execute(sql: string, params?: unknown[]): Promise<void> {
    const stmt = this.db.prepare(sql);
    try {
      await stmt.run(...(params ?? []));
      if (!this.dirty) {
        this.dirty = true;
        pendingFlush.add(this);
        scheduleFlush();
      }
    } finally {
      stmt.close();
    }
  }

  async push(): Promise<void> {
    if (!this.dirty) return;
    await this.db.push();
    this.dirty = false;
    pendingFlush.delete(this);
  }

  async pull(): Promise<void> {
    await this.db.pull();
  }

  isDirty(): boolean {
    return this.dirty;
  }
}

// ============================================================================
// Public API
// ============================================================================

export function createDb(name: string, options?: CreateDbOptions): Promise<TursoDatabase> {
  const existing = instances.get(name);
  if (existing) return existing;

  const promise = initDb(name, options);
  instances.set(name, promise);
  promise.catch(() => instances.delete(name));

  return promise;
}

// ============================================================================
// Internals
// ============================================================================

async function initDb(name: string, options?: CreateDbOptions): Promise<TursoDatabase> {
  const creds = await ensureDb(name, options?.group);
  const localPath = join(tmpdir(), `${name}.db`);
  return TursoDatabase.open(localPath, creds.url, creds.authToken, options);
}

async function ensureDb(name: string, group?: string): Promise<Credentials> {
  const cached = credentials.get(name);
  if (cached) return cached;

  const client = getClient();
  let db: { hostname?: string } | undefined;

  try {
    db = await client.databases.get(name);
  } catch (err) {
    if (isNotFound(err)) {
      db = await client.databases.create(name, { group: group ?? "default" });
    } else {
      throw err;
    }
  }

  if (!db?.hostname) {
    throw new Error(`Failed to get hostname for database: ${name}`);
  }

  const token = await client.databases.createToken(name, { authorization: "full-access" });
  const creds: Credentials = { url: `libsql://${db.hostname}`, authToken: token.jwt };
  credentials.set(name, creds);

  return creds;
}

function getClient(): ReturnType<typeof createClient> {
  const org = requireEnv("TURSO_ORG");

  if (!apiClient || apiClientOrg !== org) {
    apiClient = createClient({ org, token: requireEnv("TURSO_API_TOKEN") });
    apiClientOrg = org;
  }

  return apiClient;
}

function scheduleFlush(): void {
  if (flushScheduled) return;
  flushScheduled = true;

  waitUntil(
    (async () => {
      await Promise.resolve();
      flushScheduled = false;

      const dbs = Array.from(pendingFlush);
      pendingFlush.clear();

      await Promise.all(
        dbs.map((db) => db.push().catch((err) => console.error("Failed to push database:", err)))
      );
    })()
  );
}

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} environment variable is required`);
  return value;
}

function isNotFound(err: unknown): boolean {
  return err instanceof Error && "status" in err && (err as { status: number }).status === 404;
}

// Backwards compatibility alias
export { TursoDatabase as VercelDatabase };

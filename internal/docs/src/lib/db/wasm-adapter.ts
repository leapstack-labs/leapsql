// WASM adapter for production mode - uses sql.js-httpvfs
import type { DatabaseAdapter, QueryResult } from './types';

// Types for sql.js-httpvfs
interface SqlJsConfig {
  from: string;
  config: {
    serverMode: string;
    url: string;
    requestChunkSize: number;
  };
}

interface WorkerHttpvfs {
  db: {
    query(sql: string, params?: any[]): Promise<{
      columns?: string[];
      values?: any[][];
    }>;
  };
}

// Dynamic import for sql.js-httpvfs (only loaded in production)
let createDbWorker: ((
  configs: SqlJsConfig[],
  wasmPath: string
) => Promise<WorkerHttpvfs>) | null = null;

export class WasmAdapter implements DatabaseAdapter {
  private worker: WorkerHttpvfs | null = null;
  private initPromise: Promise<void> | null = null;

  async init(): Promise<void> {
    // Avoid double initialization
    if (this.initPromise) {
      return this.initPromise;
    }

    this.initPromise = this.doInit();
    return this.initPromise;
  }

  private async doInit(): Promise<void> {
    // Dynamically import sql.js-httpvfs
    if (!createDbWorker) {
      const module = await import('sql.js-httpvfs');
      createDbWorker = module.createDbWorker;
    }

    this.worker = await createDbWorker!(
      [{
        from: 'inline',
        config: {
          serverMode: 'full',
          url: './metadata.db',
          requestChunkSize: 4096
        }
      }],
      './sql-wasm.wasm'
    );
  }

  isReady(): boolean {
    return this.worker !== null;
  }

  async query(sql: string, params: any[] = []): Promise<QueryResult> {
    if (!this.worker) {
      throw new Error('Database not initialized');
    }

    const result = await this.worker.db.query(sql, params);
    return {
      columns: result.columns || [],
      values: result.values || []
    };
  }
}

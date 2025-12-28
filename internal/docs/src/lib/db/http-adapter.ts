// HTTP adapter for dev mode - queries the Go server
import type { DatabaseAdapter, QueryResult } from './types';

export class HttpAdapter implements DatabaseAdapter {
  private ready = false;

  async init(): Promise<void> {
    // HTTP adapter is ready immediately - server is already running
    this.ready = true;
  }

  isReady(): boolean {
    return this.ready;
  }

  async query(sql: string, params: any[] = []): Promise<QueryResult> {
    const res = await fetch('/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql, params })
    });

    if (!res.ok) {
      const errorText = await res.text();
      throw new Error(`Query failed: ${errorText}`);
    }

    return res.json();
  }
}

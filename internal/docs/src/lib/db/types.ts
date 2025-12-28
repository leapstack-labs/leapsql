// Database adapter types for SQLite-over-HTTP

export interface QueryResult {
  columns: string[];
  values: any[][];
}

export interface DatabaseAdapter {
  /** Initialize the database connection */
  init(): Promise<void>;
  
  /** Execute a SQL query with optional parameters */
  query(sql: string, params?: any[]): Promise<QueryResult>;
  
  /** Check if the adapter is ready */
  isReady(): boolean;
}

// Helper to convert query results to typed objects
export function rowsToObjects<T>(result: QueryResult): T[] {
  return result.values.map(row => {
    const obj: Record<string, any> = {};
    result.columns.forEach((col, idx) => {
      obj[col] = row[idx];
    });
    return obj as T;
  });
}

// Helper to get first row as object
export function firstRow<T>(result: QueryResult): T | null {
  if (result.values.length === 0) return null;
  const obj: Record<string, any> = {};
  result.columns.forEach((col, idx) => {
    obj[col] = result.values[0][idx];
  });
  return obj as T;
}

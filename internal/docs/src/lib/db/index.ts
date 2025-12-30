// Database module exports
export type { DatabaseAdapter, QueryResult } from './types';
export { rowsToObjects, firstRow } from './types';
export { HttpAdapter } from './http-adapter';
export { WasmAdapter } from './wasm-adapter';
export { queries } from './queries';
export type {
  QueryDef,
  ModelRow,
  ColumnRow,
  ColumnSourceRow,
  LineageEdgeRow,
  ColumnLineageNodeRow,
  ColumnLineageEdgeRow,
  SearchResultRow,
  MacroNamespaceRow,
  MacroFunctionRow,
} from './queries';

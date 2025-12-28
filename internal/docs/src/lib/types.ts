// TypeScript interfaces matching Go structs from docs.go

// Manifest types for instant shell render (from manifest.go)
export interface Manifest {
  project_name: string;
  generated_at: string;
  nav_tree: NavGroup[];
  stats: ManifestStats;
}

export interface NavGroup {
  folder: string;
  models: NavItem[];
}

export interface NavItem {
  name: string;
  path: string;
  materialized?: string;
}

export interface ManifestStats {
  model_count: number;
  source_count: number;
  column_count: number;
  folder_count: number;
  table_count: number;
  view_count: number;
}

// Global window types for embedded data
declare global {
  interface Window {
    __MANIFEST__?: Manifest;
    __DEV_MODE__?: boolean;
  }
}

export interface SourceRef {
  table: string;
  column: string;
}

export interface ColumnDoc {
  name: string;
  index: number;
  transform_type?: string;
  function?: string;
  sources: SourceRef[];
}

export interface ModelDoc {
  id: string;
  name: string;
  path: string;
  materialized: string;
  unique_key?: string;
  sql: string;
  file_path: string;
  sources: string[];
  dependencies: string[];
  dependents: string[];
  columns: ColumnDoc[];
  description?: string;
  updated_at: string;
}

export interface SourceDoc {
  name: string;
  referenced_by: string[];
}

export interface LineageEdge {
  source: string;
  target: string;
}

export interface LineageDoc {
  nodes: string[];
  edges: LineageEdge[];
}

export interface ColumnLineageNode {
  id: string;
  model: string;
  column: string;
}

export interface ColumnLineageEdge {
  source: string;
  target: string;
}

export interface ColumnLineageDoc {
  nodes: ColumnLineageNode[];
  edges: ColumnLineageEdge[];
}

export interface Catalog {
  generated_at: string;
  project_name: string;
  models: ModelDoc[];
  sources: SourceDoc[];
  lineage: LineageDoc;
  column_lineage: ColumnLineageDoc;
}

// Route types for the router
export type Route =
  | { type: 'overview' }
  | { type: 'lineage' }
  | { type: 'model'; path: string }
  | { type: 'source'; name: string }
  | { type: 'not-found' };

// Node types for React Flow
export interface ModelNodeData {
  name: string;
  folder: string;
  isSource: boolean;
  path: string;
}

export interface ColumnNodeData {
  column: string;
  model: string;
  isCurrentModel: boolean;
  isModelSource: boolean;
}

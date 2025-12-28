// Dagre layout helper for React Flow graphs
import dagre from 'dagre';
import type { Node, Edge } from '@xyflow/react';

export interface LayoutOptions {
  direction?: 'TB' | 'BT' | 'LR' | 'RL';
  nodeWidth?: number;
  nodeHeight?: number;
  rankSep?: number;
  nodeSep?: number;
}

const defaultOptions: Required<LayoutOptions> = {
  direction: 'LR',
  nodeWidth: 180,
  nodeHeight: 40,
  rankSep: 100,
  nodeSep: 30,
};

// Apply Dagre layout to nodes and edges
export function applyDagreLayout<T, E>(
  nodes: Node<T>[],
  edges: Edge<E>[],
  options: LayoutOptions = {}
): { nodes: Node<T>[]; edges: Edge<E>[] } {
  const opts = { ...defaultOptions, ...options };
  
  const g = new dagre.graphlib.Graph();
  g.setDefaultEdgeLabel(() => ({}));
  g.setGraph({
    rankdir: opts.direction,
    ranksep: opts.rankSep,
    nodesep: opts.nodeSep,
  });

  // Add nodes to the graph
  nodes.forEach(node => {
    g.setNode(node.id, {
      width: opts.nodeWidth,
      height: opts.nodeHeight,
    });
  });

  // Add edges to the graph
  edges.forEach(edge => {
    g.setEdge(edge.source, edge.target);
  });

  // Run the layout
  dagre.layout(g);

  // Apply positions to nodes
  const layoutedNodes = nodes.map(node => {
    const nodeWithPosition = g.node(node.id);
    return {
      ...node,
      position: {
        x: nodeWithPosition.x - opts.nodeWidth / 2,
        y: nodeWithPosition.y - opts.nodeHeight / 2,
      },
    };
  });

  return { nodes: layoutedNodes, edges };
}

// Convert lineage data to React Flow nodes and edges
export function lineageToFlow(
  lineageNodes: string[],
  lineageEdges: { source: string; target: string }[],
  modelsByPath: Map<string, { name: string; path: string }>
): { nodes: Node<{ name: string; folder: string; isSource: boolean; path: string }>[]; edges: Edge[] } {
  const nodes: Node<{ name: string; folder: string; isSource: boolean; path: string }>[] = lineageNodes.map(path => {
    const isSource = path.startsWith('source:');
    const displayName = isSource ? path.slice(7) : path;
    const model = isSource ? null : modelsByPath.get(path);
    const folder = isSource ? 'source' : path.split('.')[0];

    return {
      id: path,
      type: isSource ? 'source' : 'model',
      data: {
        name: model ? model.name : displayName,
        folder,
        isSource,
        path,
      },
      position: { x: 0, y: 0 }, // Will be set by layout
    };
  });

  const edges: Edge[] = lineageEdges.map((edge, index) => ({
    id: `e-${index}`,
    source: edge.source,
    target: edge.target,
    type: 'smoothstep',
    animated: edge.source.startsWith('source:'),
    style: edge.source.startsWith('source:') 
      ? { stroke: '#d29922', strokeDasharray: '4 2' }
      : undefined,
  }));

  return applyDagreLayout(nodes, edges, { direction: 'LR' });
}

// Convert column lineage data to React Flow nodes and edges
export function columnLineageToFlow(
  modelPath: string,
  columns: { name: string; sources: { table: string; column: string }[] }[],
  modelsByPath: Map<string, { name: string; path: string }>
): { nodes: Node<{ column: string; model: string; isCurrentModel: boolean; isModelSource: boolean }>[]; edges: Edge[] } {
  const nodeSet = new Map<string, { column: string; model: string; isCurrentModel: boolean; isModelSource: boolean }>();
  const edges: { source: string; target: string }[] = [];

  // Add current model's columns
  columns.forEach(col => {
    const nodeId = `${modelPath}.${col.name}`;
    nodeSet.set(nodeId, {
      column: col.name,
      model: modelPath,
      isCurrentModel: true,
      isModelSource: false,
    });

    // Add source columns and edges
    col.sources.forEach(src => {
      if (!src.table || !src.column) return;

      // Find the source model path
      let sourceModelPath = src.table;
      for (const [path, model] of modelsByPath.entries()) {
        if (model.name === src.table || path === src.table) {
          sourceModelPath = path;
          break;
        }
      }

      const sourceNodeId = `${sourceModelPath}.${src.column}`;
      const isModelSource = modelsByPath.has(sourceModelPath);

      if (!nodeSet.has(sourceNodeId)) {
        nodeSet.set(sourceNodeId, {
          column: src.column,
          model: sourceModelPath,
          isCurrentModel: false,
          isModelSource,
        });
      }

      edges.push({ source: sourceNodeId, target: nodeId });
    });
  });

  const nodes: Node<{ column: string; model: string; isCurrentModel: boolean; isModelSource: boolean }>[] = 
    Array.from(nodeSet.entries()).map(([id, data]) => ({
      id,
      type: 'column',
      data,
      position: { x: 0, y: 0 },
    }));

  return applyDagreLayout(nodes, edges.map((e, i) => ({ 
    id: `e-${i}`, 
    source: e.source, 
    target: e.target,
    type: 'smoothstep',
  })), { direction: 'LR', nodeWidth: 120, nodeHeight: 36 });
}

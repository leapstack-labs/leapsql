// Main lineage graph component using React Flow
import type { FunctionComponent } from 'preact';
import { useMemo, useCallback } from 'preact/hooks';
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  type Node,
  type Edge,
} from '@xyflow/react';
import { useLineage } from '../../lib/context';
import { lineageToFlow } from '../../lib/layout';
import { navigateToModel, navigateToSource } from '../../lib/router';
import { ModelNode } from './ModelNode';
import { SourceNode } from './SourceNode';
import '@xyflow/react/dist/style.css';

// Custom node types
const nodeTypes = {
  model: ModelNode,
  source: SourceNode,
};

interface LineageFlowProps {
  dbReady: boolean;
}

export const LineageFlow: FunctionComponent<LineageFlowProps> = ({ dbReady }) => {
  const { data: lineage, loading, error } = useLineage();

  // Convert lineage data to React Flow format
  const { initialNodes, initialEdges } = useMemo(() => {
    if (!lineage || lineage.nodes.length === 0) {
      return { initialNodes: [], initialEdges: [] };
    }

    // Create a simple modelsByPath map for layout
    // In the new architecture, we pass minimal data since layout only needs node IDs
    const modelsByPath = new Map<string, { name: string; path: string; materialized?: string }>();
    lineage.nodes.forEach(node => {
      if (!node.startsWith('source:')) {
        const parts = node.split('.');
        const name = parts[parts.length - 1];
        modelsByPath.set(node, { name, path: node });
      }
    });

    const { nodes, edges } = lineageToFlow(
      lineage.nodes,
      lineage.edges,
      modelsByPath
    );

    return { initialNodes: nodes, initialEdges: edges };
  }, [lineage]);

  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);

  // Handle node clicks
  const onNodeClick = useCallback((_event: React.MouseEvent, node: Node) => {
    const data = node.data as { isSource: boolean; name: string; path: string };
    if (data.isSource) {
      navigateToSource(data.name);
    } else {
      navigateToModel(data.path);
    }
  }, []);

  // Loading state
  if (!dbReady || loading) {
    return (
      <div class="dag-container">
        <div class="empty-state">
          <p>Loading lineage data...</p>
        </div>
      </div>
    );
  }

  // Error state
  if (error) {
    return (
      <div class="dag-container">
        <div class="empty-state">
          <p>Error loading lineage: {error.message}</p>
        </div>
      </div>
    );
  }

  if (initialNodes.length === 0) {
    return (
      <div class="dag-container">
        <div class="empty-state">
          <p>No lineage data available.</p>
        </div>
      </div>
    );
  }

  return (
    <div class="dag-container">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onNodeClick={onNodeClick}
        nodeTypes={nodeTypes}
        fitView
        fitViewOptions={{ padding: 0.2 }}
        minZoom={0.1}
        maxZoom={4}
        attributionPosition="bottom-left"
      >
        <Background color="#30363d" gap={16} />
        <Controls />
        <MiniMap
          nodeColor={(node) => {
            const data = node.data as { folder: string; isSource: boolean };
            if (data.isSource) return '#d29922';
            const colors: Record<string, string> = {
              staging: '#3fb950',
              marts: '#58a6ff',
              intermediate: '#a371f7',
              seeds: '#d29922',
            };
            return colors[data.folder] || '#8b949e';
          }}
          maskColor="rgba(0, 0, 0, 0.5)"
        />
      </ReactFlow>
    </div>
  );
};

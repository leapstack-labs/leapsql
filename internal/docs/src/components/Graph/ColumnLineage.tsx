// Column lineage graph component
import type { FunctionComponent } from 'preact';
import { useMemo, useCallback } from 'preact/hooks';
import {
  ReactFlow,
  Background,
  Controls,
  useNodesState,
  useEdgesState,
  Handle,
  Position,
  type Node,
  type NodeProps,
} from '@xyflow/react';
import { useColumnLineage } from '../../lib/context';
import type { ColumnNodeData } from '../../lib/types';
import '@xyflow/react/dist/style.css';

// Custom column node component
const ColumnNode: FunctionComponent<NodeProps> = ({ data }) => {
  const nodeData = data as ColumnNodeData;

  // Determine colors based on node type using CSS variables
  let bgColor = 'color-mix(in oklch, var(--node-source) 20%, transparent)';
  let borderColor = 'var(--node-source)';

  if (nodeData.isCurrentModel) {
    bgColor = 'color-mix(in oklch, var(--node-staging) 20%, transparent)';
    borderColor = 'var(--node-staging)';
  } else if (nodeData.isModelSource) {
    bgColor = 'color-mix(in oklch, var(--node-marts) 20%, transparent)';
    borderColor = 'var(--node-marts)';
  }

  return (
    <div class="column-lineage-node">
      <Handle
        type="target"
        position={Position.Left}
        style={{ background: borderColor }}
      />
      <div
        style={{
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          padding: '4px 8px',
          backgroundColor: bgColor,
          border: `1.5px solid ${borderColor}`,
          borderRadius: '4px',
          minWidth: '80px',
        }}
      >
        {!nodeData.isCurrentModel && (
          <span
            style={{
              fontSize: '9px',
              color: 'var(--muted-foreground)',
              marginBottom: '2px',
            }}
          >
            {nodeData.model}
          </span>
        )}
        <span
          style={{
            fontSize: '11px',
            color: 'var(--foreground)',
            fontFamily: 'SF Mono, Consolas, monospace',
          }}
        >
          {nodeData.column}
        </span>
      </div>
      <Handle
        type="source"
        position={Position.Right}
        style={{ background: borderColor }}
      />
    </div>
  );
};

// Custom node types
const nodeTypes = {
  column: ColumnNode,
};

interface ColumnLineageGraphProps {
  modelPath: string;
  dbReady: boolean;
}

export const ColumnLineageGraph: FunctionComponent<ColumnLineageGraphProps> = ({
  modelPath,
  dbReady,
}) => {
  const { data: lineage, loading, error } = useColumnLineage(modelPath);

  // Convert column lineage data to React Flow format
  const { initialNodes, initialEdges } = useMemo(() => {
    if (!lineage || lineage.nodes.length === 0) {
      return { initialNodes: [], initialEdges: [] };
    }

    // Build a set of model paths that are sources (appear in edges as sources)
    const sourceModels = new Set<string>();
    lineage.edges.forEach(edge => {
      const sourceNode = lineage.nodes.find(n => n.id === edge.source);
      if (sourceNode && sourceNode.model !== modelPath) {
        sourceModels.add(sourceNode.model);
      }
    });

    // Layout nodes in columns by model
    const nodesByModel = new Map<string, typeof lineage.nodes>();
    lineage.nodes.forEach(node => {
      if (!nodesByModel.has(node.model)) {
        nodesByModel.set(node.model, []);
      }
      nodesByModel.get(node.model)!.push(node);
    });

    // Assign x positions: current model on right, sources on left
    const modelOrder = Array.from(nodesByModel.keys()).sort((a, b) => {
      if (a === modelPath) return 1;
      if (b === modelPath) return -1;
      return a.localeCompare(b);
    });

    const columnSpacing = 200;
    const rowSpacing = 50;

    const nodes: Node[] = [];
    modelOrder.forEach((model, modelIdx) => {
      const modelNodes = nodesByModel.get(model)!;
      modelNodes.forEach((node, idx) => {
        const isCurrentModel = model === modelPath;
        const isModelSource = sourceModels.has(model);

        nodes.push({
          id: node.id,
          type: 'column',
          position: {
            x: modelIdx * columnSpacing,
            y: idx * rowSpacing,
          },
          data: {
            column: node.column,
            model: node.model,
            isCurrentModel,
            isModelSource,
          } as ColumnNodeData,
        });
      });
    });

    // Create edges
    const edges = lineage.edges.map((edge, idx) => ({
      id: `e-${idx}`,
      source: edge.source,
      target: edge.target,
      animated: false,
      style: { stroke: 'var(--muted-foreground)', strokeWidth: 1 },
    }));

    return { initialNodes: nodes, initialEdges: edges };
  }, [lineage, modelPath]);

  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);

  // Loading state
  if (!dbReady || loading) {
    return (
      <div class="column-lineage-container">
        <div class="empty-state">
          <p>Loading column lineage...</p>
        </div>
      </div>
    );
  }

  // Error state
  if (error) {
    return (
      <div class="column-lineage-container">
        <div class="empty-state">
          <p>Error loading column lineage: {error.message}</p>
        </div>
      </div>
    );
  }

  if (initialNodes.length === 0) {
    return (
      <div class="column-lineage-container">
        <div class="empty-state">
          <p>No column lineage data available for this model.</p>
        </div>
      </div>
    );
  }

  return (
    <div class="column-lineage-container">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        nodeTypes={nodeTypes}
        fitView
        fitViewOptions={{ padding: 0.2 }}
        minZoom={0.3}
        maxZoom={3}
        attributionPosition="bottom-left"
      >
        <Background color="var(--graph-bg)" gap={16} />
        <Controls />
      </ReactFlow>
    </div>
  );
};

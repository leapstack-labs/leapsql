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
import { useCatalog } from '../../lib/context';
import { columnLineageToFlow } from '../../lib/layout';
import type { ColumnNodeData } from '../../lib/types';
import '@xyflow/react/dist/style.css';

// Custom column node component
const ColumnNode: FunctionComponent<NodeProps> = ({ data }) => {
  const nodeData = data as ColumnNodeData;

  // Determine colors based on node type
  let bgColor = 'rgba(210, 153, 34, 0.2)';
  let borderColor = '#d29922';

  if (nodeData.isCurrentModel) {
    bgColor = 'rgba(63, 185, 80, 0.2)';
    borderColor = '#3fb950';
  } else if (nodeData.isModelSource) {
    bgColor = 'rgba(88, 166, 255, 0.2)';
    borderColor = '#58a6ff';
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
              color: '#8b949e',
              marginBottom: '2px',
            }}
          >
            {nodeData.model}
          </span>
        )}
        <span
          style={{
            fontSize: '11px',
            color: '#e6edf3',
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
}

export const ColumnLineageGraph: FunctionComponent<ColumnLineageGraphProps> = ({
  modelPath,
}) => {
  const { getModel, modelsByPath } = useCatalog();
  const model = getModel(modelPath);

  // Convert column lineage data to React Flow format
  const { initialNodes, initialEdges } = useMemo(() => {
    if (!model || !model.columns || model.columns.length === 0) {
      return { initialNodes: [], initialEdges: [] };
    }

    const { nodes, edges } = columnLineageToFlow(
      modelPath,
      model.columns,
      modelsByPath
    );

    return { initialNodes: nodes, initialEdges: edges };
  }, [model, modelPath, modelsByPath]);

  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);

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
        <Background color="#30363d" gap={16} />
        <Controls />
      </ReactFlow>
    </div>
  );
};

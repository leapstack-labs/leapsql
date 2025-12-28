// Custom source node for React Flow (dashed border)
import type { FunctionComponent } from 'preact';
import { Handle, Position, type NodeProps } from '@xyflow/react';
import type { ModelNodeData } from '../../lib/types';

export const SourceNode: FunctionComponent<NodeProps> = ({ data }) => {
  const nodeData = data as ModelNodeData;

  return (
    <div class="dag-node dag-node-source">
      <Handle
        type="target"
        position={Position.Left}
        style={{ background: '#d29922' }}
      />
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: '8px',
        }}
      >
        <div
          style={{
            width: '16px',
            height: '10px',
            borderRadius: '3px',
            backgroundColor: 'rgba(210, 153, 34, 0.2)',
            border: '2px dashed #d29922',
            flexShrink: 0,
          }}
        />
        <span
          style={{
            fontSize: '11px',
            color: '#e6edf3',
            whiteSpace: 'nowrap',
          }}
        >
          {nodeData.name}
        </span>
      </div>
      <Handle
        type="source"
        position={Position.Right}
        style={{ background: '#d29922' }}
      />
    </div>
  );
};

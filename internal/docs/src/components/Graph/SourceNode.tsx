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
        style={{ background: 'var(--node-source)' }}
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
            backgroundColor: 'color-mix(in oklch, var(--node-source) 20%, transparent)',
            border: '2px dashed var(--node-source)',
            flexShrink: 0,
          }}
        />
        <span
          style={{
            fontSize: '11px',
            color: 'var(--foreground)',
            whiteSpace: 'nowrap',
          }}
        >
          {nodeData.name}
        </span>
      </div>
      <Handle
        type="source"
        position={Position.Right}
        style={{ background: 'var(--node-source)' }}
      />
    </div>
  );
};

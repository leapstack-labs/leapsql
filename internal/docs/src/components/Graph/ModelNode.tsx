// Custom model node for React Flow
import type { FunctionComponent } from 'preact';
import { Handle, Position, type NodeProps } from '@xyflow/react';
import type { ModelNodeData } from '../../lib/types';

// Get node color CSS variable based on folder
function getNodeColorVar(folder: string): string {
  const varMap: Record<string, string> = {
    staging: 'var(--node-staging)',
    marts: 'var(--node-marts)',
    intermediate: 'var(--node-intermediate)',
    seeds: 'var(--node-seeds)',
  };
  return varMap[folder] || 'var(--node-default)';
}

export const ModelNode: FunctionComponent<NodeProps> = ({ data }) => {
  const nodeData = data as ModelNodeData;
  const color = getNodeColorVar(nodeData.folder);

  return (
    <div class="dag-node">
      <Handle
        type="target"
        position={Position.Left}
        style={{ background: 'var(--graph-handle)' }}
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
            width: '12px',
            height: '12px',
            borderRadius: '50%',
            backgroundColor: color,
            border: `2px solid ${color}`,
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
        style={{ background: 'var(--graph-handle)' }}
      />
    </div>
  );
};

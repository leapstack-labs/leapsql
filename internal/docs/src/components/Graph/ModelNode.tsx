// Custom model node for React Flow
import type { FunctionComponent } from 'preact';
import { Handle, Position, type NodeProps } from '@xyflow/react';
import type { ModelNodeData } from '../../lib/types';

// Get node color based on folder
function getNodeColor(folder: string): string {
  const colors: Record<string, string> = {
    staging: '#3fb950',
    marts: '#58a6ff',
    intermediate: '#a371f7',
    seeds: '#d29922',
  };
  return colors[folder] || '#8b949e';
}

export const ModelNode: FunctionComponent<NodeProps> = ({ data }) => {
  const nodeData = data as ModelNodeData;
  const color = getNodeColor(nodeData.folder);

  return (
    <div class="dag-node">
      <Handle
        type="target"
        position={Position.Left}
        style={{ background: '#30363d' }}
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
        style={{ background: '#30363d' }}
      />
    </div>
  );
};

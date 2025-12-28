// Lineage page component
import type { FunctionComponent } from 'preact';
import { LineageFlow } from '../Graph/LineageFlow';

interface LineageProps {
  dbReady: boolean;
}

export const Lineage: FunctionComponent<LineageProps> = ({ dbReady }) => {
  return (
    <>
      <div class="page-header">
        <h1>Lineage</h1>
        <p class="description">Data flow and dependencies between models</p>
      </div>

      <div class="section">
        <LineageFlow dbReady={dbReady} />
      </div>

      <div class="section">
        <h2 class="section-title">Legend</h2>
        <div
          style={{
            display: 'flex',
            gap: '2rem',
            color: 'var(--text-secondary)',
            fontSize: '0.875rem',
            flexWrap: 'wrap',
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
            <div
              style={{
                width: '12px',
                height: '12px',
                borderRadius: '50%',
                background: 'var(--node-staging)',
              }}
            />
            staging
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
            <div
              style={{
                width: '12px',
                height: '12px',
                borderRadius: '50%',
                background: 'var(--node-marts)',
              }}
            />
            marts
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
            <div
              style={{
                width: '12px',
                height: '12px',
                borderRadius: '50%',
                background: 'var(--node-default)',
              }}
            />
            other
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
            <div
              style={{
                width: '16px',
                height: '10px',
                borderRadius: '2px',
                background: 'transparent',
                border: '2px dashed var(--accent-orange)',
              }}
            />
            sources
          </div>
        </div>
      </div>
    </>
  );
};

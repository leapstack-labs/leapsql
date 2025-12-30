// Macro detail page component
import type { FunctionComponent } from 'preact';
import { useMacro } from '../../lib/context';
import { NotFound } from './NotFound';
import type { MacroFunctionDoc } from '../../lib/types';

interface MacroDetailProps {
  namespace: string;
  dbReady: boolean;
}

// Skeleton component
const Skeleton: FunctionComponent<{ width?: string; height?: string }> = ({
  width = '100%',
  height = '1em'
}) => (
  <div
    class="skeleton"
    style={{
      width,
      height,
      backgroundColor: 'var(--bg-tertiary)',
      borderRadius: '4px',
      animation: 'pulse 1.5s ease-in-out infinite'
    }}
  />
);

// Format function signature
const formatSignature = (fn: MacroFunctionDoc): string => {
  const args = fn.args.join(', ');
  return `${fn.name}(${args})`;
};

export const MacroDetail: FunctionComponent<MacroDetailProps> = ({ namespace, dbReady }) => {
  const { data: macro, loading, error } = useMacro(namespace);

  // Loading state
  if (!dbReady || loading) {
    return (
      <>
        <div class="model-header">
          <div>
            <div class="macro-badge-header">MACRO</div>
            <Skeleton width="200px" height="2rem" />
            <p style={{ marginTop: '1rem' }}>
              <Skeleton width="300px" />
            </p>
          </div>
        </div>

        <div class="section">
          <h2 class="section-title">Functions</h2>
          <div class="macro-function-list">
            <Skeleton width="100%" height="4rem" />
            <Skeleton width="100%" height="4rem" />
          </div>
        </div>
      </>
    );
  }

  // Error state
  if (error) {
    return <NotFound message={`Error loading macro: ${error.message}`} />;
  }

  // Not found
  if (!macro) {
    return <NotFound message={`Macro namespace "${namespace}" not found`} />;
  }

  return (
    <>
      <div class="model-header">
        <div>
          <div class="macro-badge-header">MACRO</div>
          <h1 class="model-title">{macro.namespace}</h1>
          <p style={{ marginTop: '0.5rem', color: 'var(--muted-foreground)' }}>
            {macro.package ? (
              <span class="macro-package">Package: {macro.package}</span>
            ) : (
              <span class="macro-package">Local macro</span>
            )}
          </p>
          <p class="model-path" style={{ marginTop: '0.5rem' }}>
            {macro.file_path}
          </p>
        </div>
      </div>

      <div class="section">
        <h2 class="section-title">
          Functions ({macro.functions.length})
        </h2>
        {macro.functions.length > 0 ? (
          <div class="macro-function-list">
            {macro.functions.map((fn) => (
              <div key={fn.name} class="macro-function-card">
                <div class="macro-function-header">
                  <code class="macro-function-signature">{formatSignature(fn)}</code>
                  {fn.line > 0 && (
                    <span class="macro-function-line">Line {fn.line}</span>
                  )}
                </div>
                {fn.docstring && (
                  <p class="macro-function-docstring">{fn.docstring}</p>
                )}
                {fn.args.length > 0 && (
                  <div class="macro-function-args">
                    <span class="args-label">Arguments:</span>
                    <div class="args-list">
                      {fn.args.map((arg, idx) => (
                        <code key={idx} class="arg-item">{arg}</code>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            ))}
          </div>
        ) : (
          <p style={{ color: 'var(--muted-foreground)', fontStyle: 'italic' }}>
            No functions defined in this macro namespace.
          </p>
        )}
      </div>

      <div class="section">
        <h2 class="section-title">About Macros</h2>
        <div class="info-box">
          <p>
            Macros are reusable Starlark functions that can be called from SQL templates
            using the <code>{'{{namespace.function()}}'}</code> syntax. They help reduce
            code duplication and enforce consistent patterns across your models.
          </p>
          <p style={{ marginTop: '0.75rem' }}>
            To use functions from this namespace, call them as{' '}
            <code>{`{{${macro.namespace}.function_name()}}`}</code> in your SQL files.
          </p>
        </div>
      </div>
    </>
  );
};

// Not found page component
import type { FunctionComponent } from 'preact';

interface NotFoundProps {
  message?: string;
}

export const NotFound: FunctionComponent<NotFoundProps> = ({ message }) => {
  return (
    <div class="empty-state">
      <h3>Not Found</h3>
      <p>{message || 'The page you are looking for does not exist.'}</p>
      <a
        href="#/"
        style={{
          color: 'var(--link-color)',
          marginTop: '1rem',
          display: 'inline-block',
        }}
      >
        Go to Overview
      </a>
    </div>
  );
};

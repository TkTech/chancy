import { ReactNode } from 'react';

interface MetricGraphCardProps {
  title: string;
  loading?: boolean;
  children: ReactNode;
  height?: number;
}

/**
 * Card wrapper for larger graph displays on dashboard
 */
export function MetricGraphCard({ title, loading, children, height = 300 }: MetricGraphCardProps) {
  return (
    <div className="card h-100">
      <div className="card-header">
        <h6 className="mb-0">{title}</h6>
      </div>
      <div className="card-body">
        {loading ? (
          <div className="d-flex align-items-center justify-content-center" style={{ height: `${height}px` }}>
            <div className="spinner-border text-secondary" role="status">
              <span className="visually-hidden">Loading...</span>
            </div>
          </div>
        ) : (
          <div style={{ height: `${height}px` }}>
            {children}
          </div>
        )}
      </div>
    </div>
  );
}

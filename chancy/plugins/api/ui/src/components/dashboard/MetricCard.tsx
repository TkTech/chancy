import { ReactNode } from 'react';

interface MetricCardProps {
  title: string;
  value: string | number;
  subtitle?: string;
  tooltip?: string;
  trend?: 'up' | 'down' | 'neutral';
  loading?: boolean;
  graphComponent?: ReactNode;
}

/**
 * Reusable metric card for dashboard displays
 */
export function MetricCard({ title, value, subtitle, tooltip, trend, loading, graphComponent }: MetricCardProps) {
  const getTrendColor = () => {
    if (!trend) return '';
    switch (trend) {
      case 'up': return 'text-success';
      case 'down': return 'text-danger';
      case 'neutral': return 'text-secondary';
      default: return '';
    }
  };

  return (
    <div className="card h-100 position-relative overflow-hidden" title={tooltip}>
      {!loading && graphComponent && (
        <div className="position-absolute top-0 start-0 w-100 h-100 opacity-25 pe-none" aria-hidden="true">
          {graphComponent}
        </div>
      )}
      <div className="card-body d-flex flex-column position-relative z-1">
        <h6 className="card-title text-uppercase text-secondary mb-3" style={{ fontSize: '0.75rem', letterSpacing: '0.05em' }}>
          {title}
        </h6>
        {loading ? (
          <div className="d-flex flex-grow-1 align-items-center justify-content-center" style={{ minHeight: '80px' }}>
            <div className="spinner-border spinner-border-sm text-secondary" role="status">
              <span className="visually-hidden">Loading...</span>
            </div>
          </div>
        ) : (
          <>
            <h2 className={`mb-2 text-break ${getTrendColor()}`}>{value}</h2>
            {subtitle && <p className="text-secondary mb-0">{subtitle}</p>}
          </>
        )}
      </div>
    </div>
  );
}

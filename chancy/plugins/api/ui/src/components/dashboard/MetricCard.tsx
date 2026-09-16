import { ReactNode } from 'react';

interface MetricCardProps {
  title: string;
  value: string | number;
  subtitle?: string;
  trend?: 'up' | 'down' | 'neutral';
  loading?: boolean;
  graphComponent?: ReactNode;
}

/**
 * Reusable metric card for dashboard displays
 */
export function MetricCard({ title, value, subtitle, trend, loading, graphComponent }: MetricCardProps) {
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
    <div className="card h-100">
      <div className="card-body">
        <h6 className="card-title text-uppercase text-secondary mb-3" style={{ fontSize: '0.75rem', letterSpacing: '0.05em' }}>
          {title}
        </h6>
        {loading ? (
          <div className="d-flex align-items-center justify-content-center" style={{ minHeight: '80px' }}>
            <div className="spinner-border spinner-border-sm text-secondary" role="status">
              <span className="visually-hidden">Loading...</span>
            </div>
          </div>
        ) : (
          <>
            <div className="d-flex align-items-baseline mb-2">
              <h2 className={`mb-0 me-2 ${getTrendColor()}`}>{value}</h2>
              {subtitle && <span className="text-secondary">{subtitle}</span>}
            </div>
            {graphComponent && (
              <div className="mt-3">
                {graphComponent}
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}

import { ReactNode } from 'react';

interface PageHeaderProps {
  title: string;
  description?: ReactNode;
  statusBadge?: ReactNode;
  actions?: ReactNode;
}

/**
 * Standardized page header component with title, optional description,
 * optional status badge, and optional action buttons/inputs
 */
export function PageHeader({ title, description, statusBadge, actions }: PageHeaderProps) {
  return (
    <div className="d-flex justify-content-between align-items-center mb-4">
      <div>
        <h2 className="mb-1">
          {statusBadge && <>{statusBadge}{' '}</>}
          {title}
        </h2>
        {description && (
          <p className="text-muted small mb-0">{description}</p>
        )}
      </div>
      {actions && (
        <div className="d-flex align-items-center">
          {actions}
        </div>
      )}
    </div>
  );
}

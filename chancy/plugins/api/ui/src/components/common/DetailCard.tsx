import { ReactNode } from 'react';

interface DetailCardProps {
  title: string;
  children: ReactNode;
}

/**
 * Generic detail section card with title and content
 */
export function DetailCard({ title, children }: DetailCardProps) {
  return (
    <div className="card mb-3">
      <div className="card-header">
        <h6 className="mb-0">{title}</h6>
      </div>
      <div className="card-body">
        {children}
      </div>
    </div>
  );
}

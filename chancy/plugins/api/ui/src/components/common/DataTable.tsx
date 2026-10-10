import { ReactNode } from 'react';

interface DataTableProps {
  children: ReactNode;
  className?: string;
}

/**
 * Wraps a table in a card with consistent styling
 */
export function DataTable({ children, className = 'table table-hover mb-0' }: DataTableProps) {
  return (
    <div className="card">
      <table className={className}>
        {children}
      </table>
    </div>
  );
}

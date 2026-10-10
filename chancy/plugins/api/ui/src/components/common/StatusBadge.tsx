import { statusToColor } from '../../utils';

interface StatusBadgeProps {
  status: string;
  className?: string;
}

/**
 * Displays a status badge with appropriate color based on status
 */
export function StatusBadge({ status, className = '' }: StatusBadgeProps) {
  const color = statusToColor(status);
  const displayText = status.charAt(0).toUpperCase() + status.slice(1);

  return (
    <span className={`badge bg-${color} ${className}`.trim()}>
      {displayText}
    </span>
  );
}

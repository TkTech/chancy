import { ReactNode, useId } from 'react';

interface DetailCardProps {
  title: string;
  children: ReactNode;
  flush?: boolean;
  className?: string;
}

export function DetailCard({ title, children, flush = false, className = '' }: DetailCardProps) {
  const titleId = useId();
  return (
    <section className={`card mb-3 ${className}`.trim()} aria-labelledby={titleId}>
      <div className="card-header"><h3 id={titleId} className="h6 mb-0">{title}</h3></div>
      <div className={flush ? 'card-body p-0' : 'card-body'}>{children}</div>
    </section>
  );
}

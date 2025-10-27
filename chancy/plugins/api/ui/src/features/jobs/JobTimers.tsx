import { useEffect, useMemo, useState } from 'react';
import type { Job } from '../../hooks/useJobs';
import { Spinner } from '../../components/Loading';

function parse(date: string | undefined): number | undefined {
  if (!date) return undefined;
  const d = new Date(date);
  return isNaN(d.getTime()) ? undefined : d.getTime();
}

function formatDuration(ms: number): string {
  if (!isFinite(ms) || ms < 0) ms = 0;
  const totalSeconds = Math.floor(ms / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  if (hours) return `${hours}h ${minutes.toString().padStart(2, '0')}m ${seconds.toString().padStart(2, '0')}s`;
  if (minutes) return `${minutes}m ${seconds.toString().padStart(2, '0')}s`;
  return `${seconds}s`;
}

function formatSinceOrUntil(target: number | undefined, now: number): string {
  if (!target) return '-';
  const diff = target - now;
  const label = diff >= 0 ? 'in ' : '';
  return label + formatDuration(Math.abs(diff));
}

function IconCircle({ variant, kind }: { variant: string, kind: 'check' | 'x' | 'spinner' | 'none' }) {
  if (kind === 'none') return <div className="icon-circle bg-secondary-subtle" aria-hidden />;
  return (
    <div className={`icon-circle bg-${variant}`} aria-hidden="true">
      {kind === 'check' && <span className="fw-bold">✓</span>}
      {kind === 'x' && <span className="fw-bold">✕</span>}
      {kind === 'spinner' && <Spinner size={28} />}
    </div>
  );
}

function Stage({ label, value, icon, tooltip }: { label: string, value: string, icon: React.ReactNode, tooltip?: string }) {
  return (
    <div className="col text-center">
      <div className="d-flex justify-content-center mb-1">{icon}</div>
      <div className="small"><strong>{label}</strong></div>
      <div className="small" title={tooltip}>{value}</div>
    </div>
  );
}

export function JobTimers({ job }: { job: Job }) {
  const [now, setNow] = useState(Date.now());
  useEffect(() => { const t = setInterval(() => setNow(Date.now()), 1000); return () => clearInterval(t); }, []);

  const createdAt = useMemo(() => parse(job.created_at), [job.created_at]);
  const startedAt = useMemo(() => parse(job.started_at), [job.started_at]);
  const completedAt = useMemo(() => parse(job.completed_at), [job.completed_at]);
  const scheduledAt = useMemo(() => parse(job.scheduled_at), [job.scheduled_at]);

  const waitingMs = useMemo(() => {
    if (!createdAt) return 0;
    const end = startedAt ?? now;
    return Math.max(0, end - createdAt);
  }, [createdAt, startedAt, now]);

  const runningMs = useMemo(() => {
    if (!startedAt) return 0;
    const end = completedAt ?? now;
    return Math.max(0, end - startedAt);
  }, [startedAt, completedAt, now]);

  // Final stage label and icon/color
  const final = useMemo(() => {
    let label = 'Completed';
    let icon: 'check' | 'x' | 'spinner' | 'none' = 'none';
    let variant = 'secondary';
    if (job.state === 'succeeded' || job.state === 'completed') {
      label = 'Succeeded'; icon = 'check'; variant = 'success';
    } else if (job.state === 'failed' || job.state === 'expired') {
      label = job.state === 'failed' ? 'Failed' : 'Expired'; icon = 'x'; variant = 'danger';
    } else if (job.state === 'retrying') {
      label = 'Retrying'; icon = 'x'; variant = 'danger';
    }
    const value = completedAt ? `${formatDuration(now - completedAt)} ago` : '-';
    return { label, icon, variant, value } as const;
  }, [job.state, completedAt, runningMs, now]);

  const isPending = job.state === 'pending';

  return (
    <div className="row row-cols-2 row-cols-md-5 g-2 align-items-center justify-content-center mb-2">
      {/* Created */}
      <Stage
        label="Created"
        value={createdAt ? `${formatDuration(now - createdAt)} ago` : '-'}
        icon={<IconCircle variant="success" kind="check" />}
        tooltip={job.created_at ? new Date(job.created_at).toLocaleString() : undefined}
      />

      {/* Scheduled */}
      <Stage
        label="Scheduled"
        value={formatSinceOrUntil(scheduledAt, now)}
        icon={scheduledAt ? <IconCircle variant={scheduledAt <= now ? 'success' : 'secondary'} kind={scheduledAt <= now ? 'check' : 'none'} /> : <IconCircle variant={'secondary'} kind={'none'} />}
        tooltip={job.scheduled_at ? new Date(job.scheduled_at).toLocaleString() : undefined}
      />

      {/* Waiting */}
      <Stage
        label="Waiting"
        value={formatDuration(waitingMs)}
        icon={startedAt ? <IconCircle variant="success" kind="check" /> : (isPending ? <IconCircle variant="primary" kind="spinner" /> : <IconCircle variant={'secondary'} kind={'none'} />)}
        tooltip={createdAt && startedAt ? `${new Date(job.created_at).toLocaleString()} → ${new Date(job.started_at).toLocaleString()}` : undefined}
      />

      {/* Running */}
      <Stage
        label="Running"
        value={startedAt ? formatDuration(runningMs) : '-'}
        icon={job.state === 'running' ? <IconCircle variant="primary" kind="spinner" /> : (startedAt ? <IconCircle variant="success" kind="check" /> : <IconCircle variant={'secondary'} kind={'none'} />)}
        tooltip={startedAt ? (completedAt ? `${new Date(job.started_at).toLocaleString()} → ${new Date(job.completed_at).toLocaleString()}` : `Started: ${new Date(job.started_at).toLocaleString()}`) : undefined}
      />

      {/* Final */}
      <Stage
        label={final.label}
        value={final.value}
        icon={<IconCircle variant={final.variant} kind={final.icon} />}
        tooltip={job.completed_at ? new Date(job.completed_at).toLocaleString() : undefined}
      />
    </div>
  );
}

import { useRef, useMemo, useState } from 'react';
import { useVirtualizer } from '@tanstack/react-virtual';
import { TimelineConfig, calculateCronExecutions, getTimelineWidth } from '../../utils/timeline';
import { TimelineAxis } from './TimelineAxis';
import { CronTimelineRow } from './CronTimelineRow';
import { useDrawer } from '../common/DrawerProvider';
import { CronDetailView } from './CronDetailView';

interface CronJob {
  unique_key: string;
  cron: string;
  next_run: string;
  last_run: string;
  job: {
    func: string;
    queue: string;
    kwargs: unknown;
    priority: number;
    max_attempts: number;
    limits: Array<{ key: string; value: number }>;
  };
}

interface CronTimelineProps {
  crons: CronJob[];
}

const COLORS = [
  '#ef4444', // red
  '#f59e0b', // amber
  '#10b981', // emerald
  '#3b82f6', // blue
  '#8b5cf6', // violet
  '#ec4899', // pink
  '#06b6d4', // cyan
  '#84cc16', // lime
];

export function CronTimeline({ crons }: CronTimelineProps) {
  const parentRef = useRef<HTMLDivElement>(null);
  const drawer = useDrawer();
  const [currentTime] = useState(new Date());

  // Timeline configuration
  const config: TimelineConfig = useMemo(() => {
    const now = new Date();
    // Show from 6 hours ago to 42 hours in the future (2 days total)
    const startTime = new Date(now.getTime() - 6 * 60 * 60 * 1000);

    return {
      pixelsPerHour: 120, // 120 pixels per hour
      startTime,
      visibleHours: 48, // Show 48 hours total
    };
  }, []);

  // Calculate executions for all crons
  const cronExecutions = useMemo(() => {
    const endTime = new Date(config.startTime.getTime() + config.visibleHours * 60 * 60 * 1000);

    return crons.map((cron, idx) => {
      const executions = calculateCronExecutions(
        cron.cron,
        config.startTime,
        endTime,
        cron.unique_key
      );

      return {
        cron,
        executions,
        color: COLORS[idx % COLORS.length]
      };
    });
  }, [crons, config]);

  // Virtual scrolling for rows
  const rowVirtualizer = useVirtualizer({
    count: cronExecutions.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 40,
    overscan: 5
  });

  const timelineWidth = getTimelineWidth(config);

  const handleCronClick = (cron: CronJob) => {
    drawer.open(
      <CronDetailView cron={cron} />,
      { title: `Scheduled Job` }
    );
  };

  return (
    <div className="card">
      <div className="card-body p-0">
        {/* Timeline controls */}
        <div className="d-flex align-items-center justify-content-between gap-2 p-2 border-bottom">
          <span className="text-muted small">
            Showing {config.visibleHours} hours • {crons.length} scheduled jobs
          </span>
          <button
            className="btn btn-sm btn-outline-secondary"
            style={{ fontSize: '0.75rem', padding: '0.25rem 0.5rem' }}
            onClick={() => {
              if (parentRef.current) {
                const now = new Date();
                const msPerHour = 1000 * 60 * 60;
                const hoursSinceStart = (now.getTime() - config.startTime.getTime()) / msPerHour;
                const scrollPosition = hoursSinceStart * config.pixelsPerHour - (parentRef.current.clientWidth / 2);
                parentRef.current.scrollLeft = Math.max(0, scrollPosition);
              }
            }}
          >
            Jump to Now
          </button>
        </div>

        {/* Timeline axis */}
        <div style={{ width: '100%', overflowX: 'auto' }} ref={parentRef}>
          <div style={{ width: `${timelineWidth + 200}px` }}>
            <div style={{ marginLeft: '200px' }}>
              <TimelineAxis config={config} currentTime={currentTime} />
            </div>

            {/* Virtual list of cron rows */}
            <div
              style={{
                height: `${Math.min(rowVirtualizer.getTotalSize(), 400)}px`,
                maxHeight: '400px',
                overflowY: 'auto',
                position: 'relative'
              }}
            >
              {rowVirtualizer.getVirtualItems().map((virtualRow) => {
                const { cron, executions, color } = cronExecutions[virtualRow.index];

                return (
                  <div
                    key={virtualRow.key}
                    style={{
                      position: 'absolute',
                      top: 0,
                      left: 0,
                      width: '100%',
                      height: `${virtualRow.size}px`,
                      transform: `translateY(${virtualRow.start}px)`
                    }}
                  >
                    <CronTimelineRow
                      cronName={cron.unique_key}
                      cronExpression={cron.cron}
                      executions={executions}
                      config={config}
                      color={color}
                      onClick={() => handleCronClick(cron)}
                    />
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

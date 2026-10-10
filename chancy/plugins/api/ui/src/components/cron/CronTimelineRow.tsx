import { TimelineConfig, CronExecution, timeToPixel } from '../../utils/timeline';

interface CronTimelineRowProps {
  cronName: string;
  cronExpression: string;
  executions: CronExecution[];
  config: TimelineConfig;
  color: string;
  onClick: () => void;
}

export function CronTimelineRow({
  cronName,
  cronExpression,
  executions,
  config,
  color,
  onClick
}: CronTimelineRowProps) {
  return (
    <div
      className="position-relative border-bottom"
      style={{
        height: '40px',
        backgroundColor: 'var(--bs-body-bg)'
      }}
    >
      {/* Job name label */}
      <div
        className="position-absolute d-flex align-items-center px-2 text-truncate small"
        style={{
          left: 0,
          top: 0,
          height: '100%',
          width: '200px',
          backgroundColor: 'var(--bs-body-bg)',
          borderRight: '1px solid var(--bs-border-color)',
          zIndex: 5
        }}
        title={`${cronName}\n${cronExpression}`}
      >
        <span className="text-truncate">{cronName}</span>
      </div>

      {/* Execution markers */}
      <div className="position-relative" style={{ marginLeft: '200px', height: '100%' }}>
        {executions.map((execution, idx) => {
          const pixel = timeToPixel(execution.time, config);
          return (
            <div
              key={idx}
              className="position-absolute"
              style={{
                left: `${pixel}px`,
                top: '8px',
                height: '24px',
                width: '2px',
                backgroundColor: color,
                cursor: 'pointer',
                transition: 'all 0.15s ease'
              }}
              onClick={onClick}
              onMouseEnter={(e) => {
                e.currentTarget.style.width = '4px';
                e.currentTarget.style.opacity = '0.8';
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.width = '2px';
                e.currentTarget.style.opacity = '1';
              }}
              title={`${cronName}\n${execution.time.toLocaleString()}`}
            />
          );
        })}
      </div>
    </div>
  );
}

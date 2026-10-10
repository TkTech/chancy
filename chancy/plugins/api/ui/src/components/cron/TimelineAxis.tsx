import { TimelineConfig, getGridMarks, timeToPixel } from '../../utils/timeline';

interface TimelineAxisProps {
  config: TimelineConfig;
  currentTime: Date;
}

export function TimelineAxis({ config, currentTime }: TimelineAxisProps) {
  const marks = getGridMarks(config);
  const currentTimePixel = timeToPixel(currentTime, config);

  return (
    <div className="position-relative" style={{ height: '40px', borderBottom: '1px solid var(--bs-border-color)' }}>
      {/* Grid marks */}
      {marks.map((mark, idx) => (
        <div
          key={idx}
          className="position-absolute"
          style={{
            left: `${mark.pixel}px`,
            top: 0,
            height: '100%',
            borderLeft: `1px solid ${mark.isMajor ? 'var(--bs-border-color)' : 'var(--bs-border-color-translucent)'}`,
          }}
        >
          <div
            className={`position-absolute ${mark.isMajor ? 'fw-semibold' : 'text-muted'}`}
            style={{
              left: '4px',
              top: mark.isMajor ? '4px' : '20px',
              fontSize: mark.isMajor ? '0.875rem' : '0.75rem',
              whiteSpace: 'nowrap'
            }}
          >
            {mark.label}
          </div>
        </div>
      ))}

      {/* Current time indicator */}
      {currentTimePixel >= 0 && currentTimePixel <= config.visibleHours * config.pixelsPerHour && (
        <div
          className="position-absolute"
          style={{
            left: `${currentTimePixel}px`,
            top: 0,
            height: '100%',
            width: '2px',
            backgroundColor: 'var(--bs-danger)',
            zIndex: 10
          }}
          title={`Current time: ${currentTime.toLocaleString()}`}
        >
          <div
            className="position-absolute bg-danger text-white px-1"
            style={{
              left: '4px',
              top: '2px',
              fontSize: '0.7rem',
              borderRadius: '2px',
              whiteSpace: 'nowrap'
            }}
          >
            Now
          </div>
        </div>
      )}
    </div>
  );
}

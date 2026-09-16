import { Cron } from 'croner';

export interface TimelineConfig {
  // Pixels per hour
  pixelsPerHour: number;
  // Start time for the timeline
  startTime: Date;
  // How many hours to show
  visibleHours: number;
}

export interface CronExecution {
  cronId: string;
  time: Date;
}

/**
 * Convert a timestamp to pixel position on the timeline
 */
export function timeToPixel(time: Date, config: TimelineConfig): number {
  const msPerHour = 1000 * 60 * 60;
  const hoursSinceStart = (time.getTime() - config.startTime.getTime()) / msPerHour;
  return hoursSinceStart * config.pixelsPerHour;
}

/**
 * Convert a pixel position to timestamp
 */
export function pixelToTime(pixel: number, config: TimelineConfig): Date {
  const msPerHour = 1000 * 60 * 60;
  const hours = pixel / config.pixelsPerHour;
  return new Date(config.startTime.getTime() + hours * msPerHour);
}

/**
 * Calculate cron executions within a time range
 */
export function calculateCronExecutions(
  cronExpression: string,
  startTime: Date,
  endTime: Date,
  cronId: string,
  maxExecutions: number = 1000
): CronExecution[] {
  try {
    const job = new Cron(cronExpression);
    const executions: CronExecution[] = [];

    let currentTime = new Date(startTime);
    let count = 0;

    while (count < maxExecutions) {
      const nextRun = job.nextRun(currentTime);
      if (!nextRun || nextRun > endTime) {
        break;
      }

      executions.push({
        cronId,
        time: nextRun
      });

      // Move to just after this execution to find the next one
      currentTime = new Date(nextRun.getTime() + 1000);
      count++;
    }

    return executions;
  } catch (error) {
    console.error(`Failed to parse cron expression "${cronExpression}":`, error);
    return [];
  }
}

/**
 * Get time grid marks for rendering the axis
 */
export interface GridMark {
  pixel: number;
  time: Date;
  label: string;
  isMajor: boolean;
}

export function getGridMarks(config: TimelineConfig): GridMark[] {
  const marks: GridMark[] = [];
  const startTime = new Date(config.startTime);

  // Round to nearest hour
  startTime.setMinutes(0, 0, 0);

  const totalHours = config.visibleHours;

  for (let i = 0; i <= totalHours; i++) {
    const time = new Date(startTime.getTime() + i * 60 * 60 * 1000);
    const pixel = timeToPixel(time, config);

    const hour = time.getHours();
    const isMajor = hour % 6 === 0; // Major marks every 6 hours

    marks.push({
      pixel,
      time,
      label: formatTimeLabel(time, isMajor),
      isMajor
    });
  }

  return marks;
}

function formatTimeLabel(time: Date, isMajor: boolean): string {
  if (isMajor) {
    // Show date and time for major marks
    return time.toLocaleString('en-US', {
      month: 'short',
      day: 'numeric',
      hour: 'numeric',
      hour12: true
    });
  } else {
    // Show just hour for minor marks
    return time.toLocaleString('en-US', {
      hour: 'numeric',
      hour12: true
    });
  }
}

/**
 * Get the total width of the timeline in pixels
 */
export function getTimelineWidth(config: TimelineConfig): number {
  return config.visibleHours * config.pixelsPerHour;
}

/**
 * Calculate the time range to render based on scroll position
 */
export function getVisibleTimeRange(
  scrollLeft: number,
  viewportWidth: number,
  config: TimelineConfig
): { start: Date; end: Date } {
  const start = pixelToTime(scrollLeft, config);
  const end = pixelToTime(scrollLeft + viewportWidth, config);

  return { start, end };
}

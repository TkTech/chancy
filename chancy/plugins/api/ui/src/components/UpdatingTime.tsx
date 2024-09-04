import { relativeTime } from '../utils';
import React from 'react';

/**
 * Renders a constantly updating time.
 *
 * @param date
 * @constructor
 */
export function UpdatingTime ({ date }: { date: string }) {
  const [time, setTime] = React.useState(relativeTime(date));

  React.useEffect(() => {
    const interval = setInterval(() => {
      setTime(relativeTime(date));
    }, 1000);

    return () => clearInterval(interval);
  }, [date]);

  return time;
}
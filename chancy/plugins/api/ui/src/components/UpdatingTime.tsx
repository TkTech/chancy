import { relativeTime } from '../utils';
import React from 'react';
import { useEffect, useState } from 'react';

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

export function CountdownTimer({ date, className }: { date: string | undefined, className?: string }) {
  const [timeString, setTimeString] = useState('');

  useEffect(() => {
    if (!date) {
      setTimeString('-');
      return;
    }

    const updateTimer = () => {
      const now = new Date().getTime();
      const target = new Date(date).getTime();
      const diff = Math.abs(target - now);
      
      const hours = Math.floor(diff / (1000 * 60 * 60));
      const minutes = Math.floor((diff % (1000 * 60 * 60)) / (1000 * 60));
      const seconds = Math.floor((diff % (1000 * 60)) / 1000);
      
      setTimeString(
        (now < target ? "+" : "-") +
        `${hours.toString().padStart(2, '0')}h` +
        `${minutes.toString().padStart(2, '0')}m` +
        `${seconds.toString().padStart(2, '0')}s`
      );
    };

    updateTimer();
    const interval = setInterval(updateTimer, 1000);
    return () => clearInterval(interval);
  }, [date]);

  return <span className={className}>{timeString}</span>;
}
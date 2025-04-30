import {useEffect, useState} from 'react';

export function relativeTime (date: string) {
  // Convert a date string to a relative time string
  // like "7 minutes ago" or "in 2 days"
  const now = new Date();
  const then = new Date(date);

  const diff = Math.abs(now.getTime() - then.getTime());
  if (diff < 1000) return 'just now';

  const was_past = then < now;

  const seconds = Math.floor(diff / 1000);
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);
  const days = Math.floor(hours / 24);
  const weeks = Math.floor(days / 7);
  const months = Math.floor(weeks / 4);
  const years = Math.floor(months / 12);

  if (years) return `${years} year${years > 1 ? 's' : ''} ${was_past ? 'ago' : 'from now'}`;
  if (months) return `${months} month${months > 1 ? 's' : ''} ${was_past ? 'ago' : 'from now'}`;
  if (weeks) return `${weeks} week${weeks > 1 ? 's' : ''} ${was_past ? 'ago' : 'from now'}`;
  if (days) return `${days} day${days > 1 ? 's' : ''} ${was_past ? 'ago' : 'from now'}`;
  if (hours) return `${hours} hour${hours > 1 ? 's' : ''} ${was_past ? 'ago' : 'from now'}`;
  if (minutes) return `${minutes} minute${minutes > 1 ? 's' : ''} ${was_past ? 'ago' : 'from now'}`;
  return `${seconds} second${seconds > 1 ? 's' : ''} ${was_past ? 'ago' : 'from now'}`;
}

export function formattedTimeDelta (start_date: string, end_date: string) {
  // Convert a date string to a relative time string
  // like "7 minutes ago" or "in 2 days"
  const now = new Date(end_date);
  const then = new Date(start_date);

  const diff = Math.abs(now.getTime() - then.getTime());

  const seconds = Math.floor(diff / 1000);
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);
  const days = Math.floor(hours / 24);
  const weeks = Math.floor(days / 7);
  const months = Math.floor(weeks / 4);
  const years = Math.floor(months / 12);

  if (years) return `${years} year${years > 1 ? 's' : ''}`;
  if (months) return `${months} month${months > 1 ? 's' : ''}`;
  if (weeks) return `${weeks} week${weeks > 1 ? 's' : ''}`;
  if (days) return `${days} day${days > 1 ? 's' : ''}`;
  if (hours) return `${hours} hour${hours > 1 ? 's' : ''}`;
  if (minutes) return `${minutes} minute${minutes > 1 ? 's' : ''}`;
  return `${seconds} second${seconds > 1 ? 's' : ''}`;
}

export function statusToColorCode (status: string) {
  return {
    pending: '#0dcaf0',
    running: '#0d6efd',
    succeeded: '#198754',
    completed: '#198754',
    failed: '#dc3545',
    expired: '#dc3545',
    retrying: '#ffc107'
  }[status] || '#6c757d';
}
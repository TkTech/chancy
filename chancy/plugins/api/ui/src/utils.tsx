export function relativeTime (date: string) {
  // Convert a date string to a relative time string
  // like "7 minutes ago"
  const now = new Date();
  const then = new Date(date);

  const diff = now.getTime() - then.getTime();
  const seconds = Math.floor(diff / 1000);
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);
  const days = Math.floor(hours / 24);
  const weeks = Math.floor(days / 7);
  const months = Math.floor(weeks / 4);
  const years = Math.floor(months / 12);

  if (years > 0) {
    return `${years} year${years > 1 ? 's' : ''} ago`;
  }

  if (months > 0) {
    return `${months} month${months > 1 ? 's' : ''} ago`;
  }

  if (weeks > 0) {
    return `${weeks} week${weeks > 1 ? 's' : ''} ago`;
  }

  if (days > 0) {
    return `${days} day${days > 1 ? 's' : ''} ago`;
  }

  if (hours > 0) {
    return `${hours} hour${hours > 1 ? 's' : ''} ago`;
  }

  if (minutes > 0) {
    return `${minutes} minute${minutes > 1 ? 's' : ''} ago`;
  }

  if (seconds > 0) {
    return `${seconds} second${seconds > 1 ? 's' : ''} ago`;
  }
}

export function statusToColor (status: string) {
  return {
    pending: 'info',
    running: 'primary',
    succeeded: 'success',
    completed: 'success',
    failed: 'danger',
    retrying: 'warning'
  }[status] || 'secondary';
}
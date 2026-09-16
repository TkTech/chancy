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

export function extractFunctionName(funcPath: string): string {
  // Extract just the function name from a full import path
  // e.g., "pastes.jobs.delete_expired_paste" -> "delete_expired_paste"
  if (!funcPath) return '';
  const parts = funcPath.split('.');
  return parts[parts.length - 1];
}

export function abbreviateFunctionName(funcPath: string, maxLength: number = 30): string {
  // Abbreviate a function path for display
  // e.g., "my.very.long.module.path.function_name" -> "m.v.l.m.p.function_name"
  if (!funcPath) return '';

  const parts = funcPath.split('.');
  if (funcPath.length <= maxLength) return funcPath;

  // Always show the full function name (last part)
  const functionName = parts[parts.length - 1];

  // If just the function name fits, return it
  if (functionName.length <= maxLength) return functionName;

  // Otherwise truncate the function name itself
  return functionName.substring(0, maxLength - 3) + '...';
}
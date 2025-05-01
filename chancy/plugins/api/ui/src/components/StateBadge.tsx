// @ts-expect-error: SVG import
import Pending from '../assets/img/pending.svg?react';
// @ts-expect-error: SVG import
import Running from '../assets/img/running.svg?react';
// @ts-expect-error: SVG import
import Succeeded from '../assets/img/succeeded.svg?react';
// @ts-expect-error: SVG import
import Failed from '../assets/img/failed.svg?react';
import {statusToColorCode} from '../utils.tsx';

export const states = [
  'pending',
  'running',
  'succeeded',
  'failed',
  'expired',
  'retrying'
] as const;

export type State = typeof states[number];

export const StatusIcons: Record<State, React.ElementType> = {
  pending: Pending,
  running: Running,
  succeeded: Succeeded,
  failed: Failed,
  expired: Failed,
  retrying: Pending
} as const;

export function StateBadge({state}: { state: string }) {
  const StatusIcon = StatusIcons[state as State] || Pending;

  return (
    <span
      className={"text-nowrap"}
      style={{
        color: statusToColorCode(state),
      }}
    >
      <StatusIcon
        width={"1em"}
        height={"1em"}
        className={'me-2'}
        style={{
          color: statusToColorCode(state),
        }}
      />
      {state}
    </span>
  )
}
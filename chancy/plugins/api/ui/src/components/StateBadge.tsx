// @ts-expect-error: SVG import
import Pending from '../assets/img/pending.svg?react';
// @ts-expect-error: SVG import
import Running from '../assets/img/running.svg?react';
// @ts-expect-error: SVG import
import Succeeded from '../assets/img/succeeded.svg?react';
// @ts-expect-error: SVG import
import Failed from '../assets/img/failed.svg?react';
import {statusToColorCode} from '../utils.tsx';

export function StateBadge({state}: { state: string }) {
  const StatusIcon = {
    pending: Pending,
    running: Running,
    succeeded: Succeeded,
    completed: Succeeded,
    failed: Failed,
    expired: Failed,
    retrying: Pending
  }[state] || Pending;

  return (
    <div
      className={'d-flex align-items-center'}
      style={{
        color: statusToColorCode(state),
      }}
    >
      <StatusIcon
        width={"24"}
        height={24}
        className={'me-2'}
        style={{
          color: statusToColorCode(state),
        }}
      />
      {state}
    </div>
  )
}
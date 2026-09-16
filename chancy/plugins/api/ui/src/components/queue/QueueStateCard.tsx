import { useState } from 'react';

interface QueueStateCardProps {
  state: string;
  resumeAt: string | null | undefined;
  onPause: (resumeAt?: string) => void;
  onResume: () => void;
  isPending: boolean;
}

export function QueueStateCard({ state, onPause, onResume, isPending }: QueueStateCardProps) {
  const [showPauseOptions, setShowPauseOptions] = useState(false);
  const [resumeType, setResumeType] = useState<'manual' | 'absolute' | 'relative'>('manual');
  const [absoluteTime, setAbsoluteTime] = useState('');
  const [relativeMinutes, setRelativeMinutes] = useState('30');

  const isActive = state === 'active';

  const handlePause = () => {
    let resumeAtIso: string | undefined;

    if (resumeType === 'absolute' && absoluteTime) {
      const d = new Date(absoluteTime);
      if (!isNaN(d.getTime())) {
        resumeAtIso = d.toISOString();
      }
    } else if (resumeType === 'relative' && relativeMinutes) {
      const minutes = parseFloat(relativeMinutes);
      if (!isNaN(minutes) && minutes > 0) {
        const resumeDate = new Date(Date.now() + minutes * 60 * 1000);
        resumeAtIso = resumeDate.toISOString();
      }
    }

    onPause(resumeAtIso);
    setShowPauseOptions(false);
    setAbsoluteTime('');
    setRelativeMinutes('30');
  };

  return (
    <>
      {!showPauseOptions ? (
        <>
          {isActive ? (
            <button
              className="btn btn-sm btn-warning"
              onClick={() => setShowPauseOptions(true)}
              disabled={isPending}
            >
              Pause
            </button>
          ) : (
            <button
              className="btn btn-sm btn-success"
              onClick={onResume}
              disabled={isPending}
            >
              Resume
            </button>
          )}
        </>
      ) : (
        <div className="position-relative">
          <div className="dropdown-menu show position-static p-3 pause-dropdown">
            <h6 className="mb-3">Pause Options</h6>

            <div className="mb-3">
              <div className="form-check">
                <input
                  className="form-check-input"
                  type="radio"
                  name="resumeType"
                  id="resumeManual"
                  checked={resumeType === 'manual'}
                  onChange={() => setResumeType('manual')}
                />
                <label className="form-check-label" htmlFor="resumeManual">
                  Manual resume (pause indefinitely)
                </label>
              </div>
              <div className="form-check">
                <input
                  className="form-check-input"
                  type="radio"
                  name="resumeType"
                  id="resumeRelative"
                  checked={resumeType === 'relative'}
                  onChange={() => setResumeType('relative')}
                />
                <label className="form-check-label" htmlFor="resumeRelative">
                  Resume after
                </label>
              </div>
              {resumeType === 'relative' && (
                <div className="ms-4 mt-2">
                  <div className="input-group input-group-sm pause-input-sm">
                    <input
                      type="number"
                      className="form-control"
                      value={relativeMinutes}
                      onChange={e => setRelativeMinutes(e.target.value)}
                      min="1"
                      step="1"
                    />
                    <span className="input-group-text">minutes</span>
                  </div>
                </div>
              )}
              <div className="form-check">
                <input
                  className="form-check-input"
                  type="radio"
                  name="resumeType"
                  id="resumeAbsolute"
                  checked={resumeType === 'absolute'}
                  onChange={() => setResumeType('absolute')}
                />
                <label className="form-check-label" htmlFor="resumeAbsolute">
                  Resume at specific time
                </label>
              </div>
              {resumeType === 'absolute' && (
                <div className="ms-4 mt-2">
                  <input
                    type="datetime-local"
                    className="form-control form-control-sm pause-input-datetime"
                    value={absoluteTime}
                    onChange={e => setAbsoluteTime(e.target.value)}
                  />
                </div>
              )}
            </div>

            <div className="d-flex gap-2">
              <button
                className="btn btn-sm btn-warning"
                onClick={handlePause}
                disabled={isPending}
              >
                {isPending ? 'Pausing...' : 'Pause'}
              </button>
              <button
                className="btn btn-sm btn-secondary"
                onClick={() => {
                  setShowPauseOptions(false);
                  setResumeType('manual');
                  setAbsoluteTime('');
                  setRelativeMinutes('30');
                }}
                disabled={isPending}
              >
                Cancel
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  );
}

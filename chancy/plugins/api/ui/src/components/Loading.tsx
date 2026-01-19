import { withBasePath } from '../config.ts';

const logoPath = withBasePath('/logo_small.png');

export function Loading () {
  return (
    <div className={"d-flex align-items-center justify-content-center p-3"} role="status" aria-live="polite" aria-busy="true">
      <img src={logoPath} alt="Loading..." className="chancy-spinner" />
      <span className="visually-hidden">Loading...</span>
    </div>
  );
}

export function Spinner({ size = 16, className = '' }: { size?: number, className?: string }) {
  return (
    <img
      src={logoPath}
      alt=""
      aria-hidden
      className={`chancy-spinner ${className}`}
      style={{ width: size, height: size }}
    />
  );
}

export function Loading () {
  return (
    <div className={"d-flex align-items-center justify-content-center p-3"} role="status" aria-live="polite" aria-busy="true">
      <img src="/logo_small.png" alt="Loading..." className="chancy-spinner" />
      <span className="visually-hidden">Loading...</span>
    </div>
  );
}

export function Spinner({ size = 16, className = '' }: { size?: number, className?: string }) {
  return (
    <img
      src="/logo_small.png"
      alt=""
      aria-hidden
      className={`chancy-spinner ${className}`}
      style={{ width: size, height: size }}
    />
  );
}

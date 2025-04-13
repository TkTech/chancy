export function Loading () {
  return (
    <div className={"d-flex align-items-center justify-content-center"}>
      <div className={"spinner-border"} role={"status"}>
        <span className={"visually-hidden"}>Loading...</span>
      </div>
    </div>
  );
}

export function LoadingWithMessage ({message}: { message: string }) {
  return (
    <div className={"d-flex flex-column align-items-center justify-content-center"}>
      <div className={"spinner-border me-2"} role={"status"}>
        <span className={"visually-hidden"}>Loading...</span>
      </div>
      <div className={"text-muted mt-2"}>{message}</div>
    </div>
  );
}
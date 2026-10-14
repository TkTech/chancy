export function getToken(serverUrl: string): string | null {
  try {
    return localStorage.getItem(`auth.token:${serverUrl}`);
  } catch {
    return null;
  }
}

export function setToken(serverUrl: string, token: string) {
  try {
    localStorage.setItem(`auth.token:${serverUrl}`, token);
  } catch { /* Storage may be disabled by the browser. */ }
}

export function clearToken(serverUrl: string) {
  try {
    localStorage.removeItem(`auth.token:${serverUrl}`);
  } catch { /* Storage may be disabled by the browser. */ }
}

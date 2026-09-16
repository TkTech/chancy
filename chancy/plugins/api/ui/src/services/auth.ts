export function getToken(): string | null {
  try {
    return localStorage.getItem('auth.token');
  } catch {
    return null;
  }
}

export function setToken(token: string) {
  try {
    localStorage.setItem('auth.token', token);
  } catch {}
}

export function clearToken() {
  try {
    localStorage.removeItem('auth.token');
  } catch {}
}


// The server injects a mount-aware base into index.html. In Vite development
// this is simply "/", and its API/WebSocket proxy uses the same origin.
export const dashboardUrl = new URL(document.baseURI);
export const dashboardBasePath = dashboardUrl.pathname.replace(/\/+$/, '');

export function normalizeServerUrl(value: string): string | null {
  try {
    const url = new URL(value);
    if (!['http:', 'https:'].includes(url.protocol) ||
        url.username || url.password || url.search || url.hash) {
      return null;
    }
    return `${url.origin}${url.pathname.replace(/\/+$/, '')}`;
  } catch {
    return null;
  }
}

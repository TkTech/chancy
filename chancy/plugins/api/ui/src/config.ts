declare global {
  interface Window {
    __CHANCY_BASE_PATH__?: string;
  }
}

export function normalizeBasePath(path?: string | null): string {
  if (!path) return "";

  const trimmed = path.trim();
  if (!trimmed || trimmed === "/") return "";

  const stripped = trimmed.replace(/\/+$/, "");
  if (!stripped) return "";

  return stripped.startsWith("/") ? stripped : `/${stripped}`;
}

export function getConfiguredBasePath(): string {
  return normalizeBasePath(window.__CHANCY_BASE_PATH__ || "");
}

export function withBasePath(path: string): string {
  const base = getConfiguredBasePath();
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;

  if (!base) {
    return normalizedPath;
  }

  return `${base}${normalizedPath}`;
}

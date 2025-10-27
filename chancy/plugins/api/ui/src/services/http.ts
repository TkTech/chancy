import { z } from 'zod';
import { getToken } from './auth';

export class ApiError extends Error {
  status: number;
  title?: string;
  detail?: string;
  body?: unknown;

  constructor(status: number, message: string, body?: unknown) {
    super(message);
    this.status = status;
    this.name = 'ApiError';
    // Try to map RFC7807-style errors
    if (body && typeof body === 'object') {
      const anyBody = body as Record<string, unknown>;
      this.title = (anyBody.title as string | undefined) || undefined;
      this.detail = (anyBody.detail as string | undefined) || undefined;
    }
    this.body = body;
  }
}

export interface RequestOptions {
  method?: string;
  body?: unknown;
  signal?: AbortSignal;
}

export async function request<T>(baseUrl: string, path: string, options: RequestOptions = {}): Promise<T> {
  const url = `${baseUrl}${path}`;

  const init: RequestInit = {
    method: options.method || 'GET',
    credentials: 'include',
    headers: {
      'Accept': 'application/json',
      'Content-Type': 'application/json',
    },
    signal: options.signal,
  };

  // Attach bearer token if available
  const token = getToken();
  if (token) {
    (init.headers as Record<string, string>)["Authorization"] = `Bearer ${token}`;
  }

  if (options.body !== undefined) {
    init.body = JSON.stringify(options.body);
  }

  const res = await fetch(url, init);
  const text = await res.text();
  const json = text ? safeJsonParse(text) : undefined;

  if (!res.ok) {
    const msg = (json && (json.title || json.detail)) || res.statusText || 'Request failed';
    throw new ApiError(res.status, msg as string, json);
  }

  return json as T;
}

function safeJsonParse(text: string) {
  try { return JSON.parse(text); } catch { return undefined; }
}

// Small helpers to validate server payloads as needed
export function parseWith<T>(schema: z.ZodType<T>, data: unknown): T {
  const result = schema.safeParse(data);
  if (!result.success) {
    // eslint-disable-next-line no-console
    console.error('Response validation failed:', result.error.format());
    throw new Error('Invalid server response shape');
  }
  return result.data;
}

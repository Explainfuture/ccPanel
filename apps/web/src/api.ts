export class ApiError extends Error {
  constructor(
    public code: string,
    public status: number,
  ) {
    super(code);
  }
}
let csrf = "";
export const setCsrf = (value: string) => {
  csrf = value;
};
export async function api<T>(
  path: string,
  options: RequestInit = {},
): Promise<T> {
  const response = await fetch(path, {
    ...options,
    credentials: "same-origin",
    headers: {
      "content-type": "application/json",
      ...(csrf ? { "x-csrf-token": csrf } : {}),
      ...options.headers,
    },
  });
  const data = await response
    .json()
    .catch(() => ({ error: "invalid_response" }));
  if (!response.ok)
    throw new ApiError(data.error || "request_failed", response.status);
  return data as T;
}

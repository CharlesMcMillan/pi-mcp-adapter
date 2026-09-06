import type { FetchLike } from "@modelcontextprotocol/client";
import { currentAuthTransaction } from "./mcp-auth.ts";
import { combineAbortSignals } from "./runtime-owner.ts";

function resolveOAuthRequestTimeoutMs(): number {
  const parsed = Number(process.env.PI_MCP_OAUTH_REQUEST_TIMEOUT_MS);
  return Number.isSafeInteger(parsed) && parsed > 0 && parsed <= 2_147_483_647 ? parsed : 30_000;
}

export function authFetch(signal?: AbortSignal, baseFetch: FetchLike = fetch): FetchLike {
  return (url, init) => {
    const timeoutSignal = AbortSignal.timeout(resolveOAuthRequestTimeoutMs());
    const combined = combineAbortSignals(signal, timeoutSignal, init?.signal ?? undefined);
    return baseFetch(url, { ...init, ...(combined ? { signal: combined } : {}) });
  };
}

export function createOAuthAwareFetch(baseFetch: FetchLike = fetch): FetchLike {
  return (url, init) => {
    const transaction = currentAuthTransaction();
    return transaction ? authFetch(transaction.signal, baseFetch)(url, init) : baseFetch(url, init);
  };
}

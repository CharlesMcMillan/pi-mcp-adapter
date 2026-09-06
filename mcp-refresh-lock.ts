/**
 * Cross-process serialized refresh lock.
 *
 * Serializes the refresh transaction (read tokens -> redeem -> persist rotated
 * tokens) across concurrent processes sharing one MCP server credential.
 * Without this, two processes redeem the same single-use rotating refresh
 * token and the loser receives `invalid_grant`, forcing an interactive re-auth.
 *
 * Implementation: one fixed lock directory per server. `mkdir` claims the
 * directory, and a nonce holder file published with `wx` resolves the small
 * mkdir → holder-publication window without ever allowing two holders. Dead
 * holders are reclaimed by pid liveness; live holders are never age-stolen.
 */

import { createHash, randomBytes } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { join } from "node:path";

export interface RefreshLock {
  release(): void;
}

interface Holder {
  pid: number;
  acquiredAt: number;
  nonce: string;
}

const DEFAULT_OAUTH_REQUEST_TIMEOUT_MS = 30_000;
const MAX_OAUTH_REQUEST_TIMEOUT_MS = 2_147_483_647;
const LOCK_WAIT_GRACE_MS = 5_000;
const HOLDER_PUBLISH_GRACE_MS = 5_000;
const RETRY_BASE_MS = 50;
const RETRY_MAX_MS = 250;

function lockDir(serverName: string, baseDir: string): string {
  const key = createHash("sha256").update(serverName, "utf8").digest("hex");
  return join(baseDir, "locks", key);
}

function holderPath(lockDir: string): string {
  return join(lockDir, "holder.json");
}

function resolveMaxWaitMs(): number {
  const raw = process.env.PI_MCP_OAUTH_REQUEST_TIMEOUT_MS;
  const parsed = raw === undefined ? Number.NaN : Number(raw);
  const requestTimeoutMs = Number.isSafeInteger(parsed) && parsed > 0 && parsed <= MAX_OAUTH_REQUEST_TIMEOUT_MS
    ? parsed
    : DEFAULT_OAUTH_REQUEST_TIMEOUT_MS;
  return Math.min(requestTimeoutMs + LOCK_WAIT_GRACE_MS, MAX_OAUTH_REQUEST_TIMEOUT_MS);
}

function readHolder(dir: string): Holder | undefined {
  try {
    return JSON.parse(readFileSync(holderPath(dir), "utf8")) as Holder;
  } catch {
    return undefined;
  }
}

function holderAlive(holder: Holder): boolean {
  try {
    process.kill(holder.pid, 0);
    return true;
  } catch (error) {
    const err = error as NodeJS.ErrnoException;
    // ESRCH: process is dead. EPERM: alive, owned by another user.
    return err.code === "EPERM";
  }
}

function holderPublicationGraceElapsed(dir: string): boolean {
  try {
    return Date.now() - statSync(dir).mtimeMs > HOLDER_PUBLISH_GRACE_MS;
  } catch {
    return true;
  }
}

function claimHolderFile(dir: string, holder: Holder): boolean {
  try {
    writeFileSync(holderPath(dir), JSON.stringify(holder), { flag: "wx" });
    return readHolder(dir)?.nonce === holder.nonce;
  } catch {
    return false;
  }
}

export async function acquireRefreshLock(serverName: string, baseDir: string): Promise<RefreshLock> {
  const dir = lockDir(serverName, baseDir);
  const locksRoot = join(baseDir, "locks");
  mkdirSync(locksRoot, { recursive: true });
  const maxWaitMs = resolveMaxWaitMs();
  const deadline = Date.now() + maxWaitMs;

  for (;;) {
    let createdLockDir = false;
    if (!existsSync(dir)) {
      try {
        mkdirSync(dir);
        createdLockDir = true;
      } catch {
        // Another process won the fixed directory; fall through to holder check.
      }
    }

    const holder = readHolder(dir);
    if (holder !== undefined) {
      if (!holderAlive(holder)) {
        try { rmSync(dir, { recursive: true, force: true }); } catch {}
        continue;
      }
    } else if (existsSync(holderPath(dir))) {
      // A partial/corrupt holder file can only come from a crashed writer.
      // Remove it after the publication grace window, then claim with `wx`.
      if (holderPublicationGraceElapsed(dir)) {
        try { rmSync(holderPath(dir), { force: true }); } catch {}
      }
    } else if (createdLockDir || holderPublicationGraceElapsed(dir)) {
      // Either we just created the directory, or a previous acquirer crashed
      // before publishing. Claim the fixed directory without deleting it: the
      // `wx` holder file ensures only one process can complete publication.
      const nonce = randomBytes(8).toString("hex");
      if (claimHolderFile(dir, { pid: process.pid, acquiredAt: Date.now(), nonce })) {
        return {
          release() {
            try {
              const current = readHolder(dir);
              if (current?.nonce === nonce) {
                rmSync(dir, { recursive: true, force: true });
              }
            } catch {}
          },
        };
      }
    }

    if (Date.now() > deadline) {
      throw new Error(`Refresh lock for ${serverName} not acquired within ${maxWaitMs}ms`);
    }
    await new Promise((resolve) => setTimeout(resolve, RETRY_BASE_MS + Math.floor(Math.random() * (RETRY_MAX_MS - RETRY_BASE_MS))));
  }
}

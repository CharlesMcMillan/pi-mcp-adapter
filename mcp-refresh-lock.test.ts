import { describe, it } from "node:test"
import assert from "node:assert"
import { createHash } from "node:crypto"
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs"
import { join } from "node:path"
import { tmpdir } from "node:os"

import { acquireRefreshLock } from "./mcp-refresh-lock.ts"

function lockPrefix(serverName: string): string {
  return `${createHash("sha256").update(serverName, "utf8").digest("hex")}.`
}

describe("mcp-refresh-lock", () => {
  it("serializes concurrent holders until release", async () => {
    const baseDir = mkdtempSync(join(tmpdir(), "mcp-refresh-lock-"))
    try {
      const first = await acquireRefreshLock("server", baseDir)
      let secondAcquired = false
      const secondPromise = acquireRefreshLock("server", baseDir).then(lock => {
        secondAcquired = true
        return lock
      })

      await new Promise(resolve => setTimeout(resolve, 300))
      assert.strictEqual(secondAcquired, false)
      first.release()

      const second = await secondPromise
      assert.strictEqual(secondAcquired, true)
      second.release()
    } finally {
      rmSync(baseDir, { recursive: true, force: true })
    }
  })

  it("reclaims a lock whose holder process is dead", async () => {
    const baseDir = mkdtempSync(join(tmpdir(), "mcp-refresh-lock-"))
    try {
      const serverName = "dead-holder"
      const deadHolderDir = join(baseDir, "locks", lockPrefix(serverName).slice(0, -1))
      mkdirSync(deadHolderDir, { recursive: true })
      writeFileSync(join(deadHolderDir, "holder.json"), JSON.stringify({
        pid: 99999999,
        acquiredAt: Date.now(),
        nonce: "0123456789abcdef",
      }))

      const lock = await acquireRefreshLock(serverName, baseDir)
      lock.release()
    } finally {
      rmSync(baseDir, { recursive: true, force: true })
    }
  })
})

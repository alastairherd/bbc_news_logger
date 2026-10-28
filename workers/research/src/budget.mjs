import { DurableObject } from "cloudflare:workers";
import { reserveSpend, settleSpend } from "./core.mjs";

// A single named instance serialises every reservation, so Worker isolates running in parallel
// across Cloudflare's network share one ledger and cannot spend past the daily limit together.
export class AnswerBudget extends DurableObject {
  async reserve(day, amountUsd, limitUsd) {
    const result = reserveSpend(await this.ctx.storage.get("ledger"), { day, amountUsd, limitUsd });
    if (result.ok) await this.ctx.storage.put("ledger", result.ledger);
    return { ok: result.ok, spentUsd: result.ledger.spentUsd };
  }

  async settle(day, reservedUsd, actualUsd) {
    const ledger = settleSpend(await this.ctx.storage.get("ledger"), { day, reservedUsd, actualUsd });
    if (ledger) await this.ctx.storage.put("ledger", ledger);
  }

  async spent(day) {
    const ledger = await this.ctx.storage.get("ledger");
    return ledger?.day === day ? ledger.spentUsd : 0;
  }
}

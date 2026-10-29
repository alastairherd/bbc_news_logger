// GitHub drops many scheduled runs, so a Cloudflare cron starts collection by workflow dispatch.
const WORKFLOWS_URL = "https://api.github.com/repos/alastairherd/bbc_news_logger/actions/workflows";
export const COLLECTION_DISPATCH_URL = `${WORKFLOWS_URL}/scrape_bbc.yml/dispatches`;
export const ALERT_DISPATCH_URL = `${WORKFLOWS_URL}/collection_alert.yml/dispatches`;
// Every published capture is one Hugging Face commit with this title prefix.
export const DATASET_COMMITS_URL = "https://huggingface.co/api/datasets/AlastairH/bbc-news-logger/commits/main?limit=100";
const CAPTURE_COMMIT_PREFIX = "Update observations for ";
export const STALE_AFTER_MS = 3 * 60 * 60 * 1000;

async function dispatchWorkflow(url, token, fetchImpl, name) {
  if (!token) {
    console.warn(`DISPATCH_TOKEN is not configured; ${name} was not dispatched.`);
    return false;
  }
  const response = await fetchImpl(url, {
    method: "POST",
    headers: {
      "Accept": "application/vnd.github+json",
      "Authorization": `Bearer ${token}`,
      "Content-Type": "application/json",
      "User-Agent": "bbc-news-archive-research-worker",
      "X-GitHub-Api-Version": "2022-11-28",
    },
    body: JSON.stringify({ ref: "main" }),
  });
  if (!response.ok) {
    const detail = (await response.text()).slice(0, 300);
    throw new Error(`GitHub dispatch failed with HTTP ${response.status}: ${detail}`);
  }
  return true;
}

export function dispatchCollection(token, fetchImpl = fetch) {
  return dispatchWorkflow(COLLECTION_DISPATCH_URL, token, fetchImpl, "hourly collection");
}

// Commits arrive newest first. When none of them is a capture, the oldest one still bounds how
// long ago the last capture could have been, so a busy dataset can never look falsely stale.
export function collectionState(commits, now = new Date()) {
  const capture = commits.find((commit) => String(commit?.title ?? "").startsWith(CAPTURE_COMMIT_PREFIX));
  const reference = capture ?? commits.at(-1);
  const time = Date.parse(reference?.date ?? "");
  if (!Number.isFinite(time)) return { stale: false, lastCaptureAt: null };
  const ageMs = now.getTime() - time;
  if (!capture) return { stale: ageMs > STALE_AFTER_MS, lastCaptureAt: null };
  return { stale: ageMs > STALE_AFTER_MS, lastCaptureAt: new Date(time).toISOString() };
}

// The alert workflow opens the GitHub issue, so the dispatch token needs no issue permissions.
export async function checkCollection(token, fetchImpl = fetch, now = new Date()) {
  const response = await fetchImpl(DATASET_COMMITS_URL, { headers: { "Accept": "application/json" } });
  if (!response.ok) throw new Error(`Hugging Face commit listing failed with HTTP ${response.status}`);
  const state = collectionState(await response.json(), now);
  if (!state.stale) return { ...state, alerted: false };
  console.warn(`No capture since ${state.lastCaptureAt ?? "before the listed commits"}; dispatching the collection alert.`);
  return { ...state, alerted: await dispatchWorkflow(ALERT_DISPATCH_URL, token, fetchImpl, "collection alert") };
}

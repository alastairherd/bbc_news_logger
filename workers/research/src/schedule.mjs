// GitHub drops many scheduled runs, so a Cloudflare cron starts collection by workflow dispatch.
export const COLLECTION_DISPATCH_URL =
  "https://api.github.com/repos/alastairherd/bbc_news_logger/actions/workflows/scrape_bbc.yml/dispatches";

export async function dispatchCollection(token, fetchImpl = fetch) {
  if (!token) {
    console.warn("DISPATCH_TOKEN is not configured; hourly collection was not dispatched.");
    return false;
  }
  const response = await fetchImpl(COLLECTION_DISPATCH_URL, {
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

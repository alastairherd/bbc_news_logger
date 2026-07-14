export {};

interface TrendRow {
  observed_date: string;
  dimension: string;
  value: string;
  article_count: number;
}

interface TrendSeriesRow {
  observed_date: string;
  article_count: number;
}

interface TrendBucket {
  counts: Map<string, number>;
  total: number;
  recent: number;
}

interface EventArticle {
  story_id: string;
  title: string;
  url: string;
  fetched_at: string;
  best_position: number | null;
  surfaces: string[];
}

interface EventCluster {
  cluster_id: string;
  label: string;
  event_type: string;
  themes: string[];
  first_seen: string;
  last_seen: string;
  article_count: number;
  version_count: number;
  articles: EventArticle[];
}

const TREND_DAYS = 120;
const VALUE_LIMIT = 150;
const trendIndex = new Map<string, Map<string, TrendBucket>>();
let latestTrendDate = "";
let recentTrendStart = "";
let events: EventCluster[] = [];

function send(type: string, payload: Record<string, unknown> = {}) {
  self.postMessage({ type, ...payload });
}

function shiftDate(value: string, days: number): string {
  const date = new Date(`${value}T00:00:00Z`);
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
}

function valuesForDimension(dimension: string): { values: string[]; available: number } {
  const buckets = trendIndex.get(dimension) ?? new Map<string, TrendBucket>();
  const values = [...buckets]
    .filter(([value]) => value && value !== "unlabelled")
    .sort(([leftValue, left], [rightValue, right]) =>
      right.recent - left.recent ||
      right.total - left.total ||
      leftValue.localeCompare(rightValue),
    )
    .slice(0, VALUE_LIMIT)
    .map(([value]) => value);
  return { values, available: buckets.size };
}

function trendSeries(dimension: string, value: string): TrendSeriesRow[] {
  const bucket = trendIndex.get(dimension)?.get(value);
  if (!latestTrendDate) return [];
  return Array.from({ length: TREND_DAYS }, (_, index) => {
    const observedDate = shiftDate(latestTrendDate, index - TREND_DAYS + 1);
    return {
      observed_date: observedDate,
      article_count: bucket?.counts.get(observedDate) ?? 0,
    };
  });
}

async function loadTrends(url: string) {
  send("trend-status", { message: "Downloading daily trend data…" });
  const response = await fetch(url);
  if (!response.ok) throw new Error("Trend mart unavailable");
  const rows = await response.json() as TrendRow[];
  latestTrendDate = rows.reduce(
    (latest, row) => row.observed_date > latest ? row.observed_date : latest,
    "",
  );
  recentTrendStart = latestTrendDate ? shiftDate(latestTrendDate, -(TREND_DAYS - 1)) : "";
  send("trend-status", { message: "Indexing active signals…" });
  for (const row of rows) {
    let dimension = trendIndex.get(row.dimension);
    if (!dimension) {
      dimension = new Map();
      trendIndex.set(row.dimension, dimension);
    }
    let bucket = dimension.get(row.value);
    if (!bucket) {
      bucket = { counts: new Map(), total: 0, recent: 0 };
      dimension.set(row.value, bucket);
    }
    bucket.counts.set(row.observed_date, row.article_count);
    bucket.total += row.article_count;
    if (row.observed_date >= recentTrendStart) bucket.recent += row.article_count;
  }
  const { values, available } = valuesForDimension("topic");
  send("trend-ready", {
    dimension: "topic",
    values,
    available,
    latestDate: latestTrendDate,
    days: TREND_DAYS,
    limit: VALUE_LIMIT,
  });
}

function eventSummary(event: EventCluster) {
  return {
    cluster_id: event.cluster_id,
    label: event.label,
    event_type: event.event_type,
    themes: event.themes,
    last_seen: event.last_seen,
    article_count: event.article_count,
  };
}

function eventPage(query: string, page: number, pageSize: number) {
  const normalized = query.trim().toLocaleLowerCase();
  const matches = events.filter((event) =>
    !normalized || `${event.label} ${event.event_type} ${event.themes.join(" ")}`
      .toLocaleLowerCase()
      .includes(normalized),
  );
  const pageCount = Math.max(1, Math.ceil(matches.length / pageSize));
  const safePage = Math.max(0, Math.min(page, pageCount - 1));
  const first = safePage * pageSize;
  send("event-page", {
    query,
    page: safePage,
    pageSize,
    total: matches.length,
    events: matches.slice(first, first + pageSize).map(eventSummary),
  });
}

async function loadEvents(url: string) {
  send("event-status", { message: "Downloading recurring-story timelines…" });
  const response = await fetch(url);
  if (!response.ok) throw new Error("Recurring-story mart unavailable");
  events = await response.json() as EventCluster[];
  send("events-ready", { count: events.length });
}

self.addEventListener("message", (event: MessageEvent) => {
  const message = event.data as Record<string, unknown>;
  if (message.type === "init-trends") {
    void loadTrends(String(message.url)).catch((error) => send("trend-error", {
      message: error instanceof Error ? error.message : "Trend data could not be prepared.",
    }));
    return;
  }
  if (message.type === "trend-values") {
    const dimension = String(message.dimension ?? "topic");
    send("trend-values", { dimension, ...valuesForDimension(dimension), limit: VALUE_LIMIT });
    return;
  }
  if (message.type === "trend-series") {
    const dimension = String(message.dimension ?? "topic");
    const value = String(message.value ?? "");
    send("trend-series", {
      requestId: Number(message.requestId ?? 0),
      dimension,
      value,
      rows: trendSeries(dimension, value),
      latestDate: latestTrendDate,
      days: TREND_DAYS,
    });
    return;
  }
  if (message.type === "init-events") {
    void loadEvents(String(message.url)).catch((error) => send("events-error", {
      message: error instanceof Error ? error.message : "Recurring stories could not be prepared.",
    }));
    return;
  }
  if (message.type === "event-page") {
    eventPage(
      String(message.query ?? ""),
      Number(message.page ?? 0),
      Math.max(1, Number(message.pageSize ?? 40)),
    );
    return;
  }
  if (message.type === "event-detail") {
    const id = String(message.id ?? "");
    send("event-detail", { event: events.find((row) => row.cluster_id === id) ?? null });
  }
});

export interface SemanticDocument {
  story_id: string;
  content_sha256: string;
  title: string;
  url: string;
  fetched_at: string;
  first_seen?: string;
  surfaces: string[];
  best_position: number | null;
  front_page_captures?: number;
  most_read_captures?: number;
  best_front_page?: number | null;
  best_most_read?: number | null;
  cluster_index?: number | null;
  cluster_id?: string | null;
  summary: string;
  topic: string;
  themes: string[];
  story_form: string;
  event_type: string;
  named_entities: string[];
  scale: number;
  norm: number;
}

export interface OntologyPromotionStats {
  settled_stories: number;
  promoted_only: number;
  read_only: number;
  both: number;
  reach_rate: number | null;
  unpromoted_share: number | null;
  median_lag_minutes: number | null;
}

export interface OntologyTheme {
  name: string;
  aliases: string[];
  stories: number;
  front_page_stories: number;
  most_read_stories: number;
  promotion: OntologyPromotionStats | null;
}

export interface OntologyEntity {
  name: string;
  aliases: string[];
  stories: number;
  front_page_stories: number;
  most_read_stories: number;
  top_themes: string[];
}

export interface OntologyEvent {
  cluster_id: string;
  label: string;
  event_type: string;
  themes: string[];
  first_seen: string;
  last_seen: string;
  article_count: number;
}

export interface SemanticOntology {
  schemaVersion: number;
  ontologyVersion: string;
  contracts?: Record<string, string>;
  themes: OntologyTheme[];
  entities: OntologyEntity[];
  events: OntologyEvent[];
}

export interface SearchFilters {
  surface?: string;
  topic?: string;
  storyForm?: string;
}

export interface MatchedThemeRecord extends OntologyTheme {
  id: string;
  filter_matches?: number;
}

export interface MatchedEntityRecord extends OntologyEntity {
  id: string;
  filter_matches?: number;
}

export interface MatchedEventRecord extends OntologyEvent {
  id: string;
}

export interface OntologyContext {
  ontologyVersion: string;
  activeFilters: SearchFilters;
  matchedConceptStoryCount: number;
  themes: MatchedThemeRecord[];
  entities: MatchedEntityRecord[];
  events: MatchedEventRecord[];
}

export interface SearchResultItem {
  document: SemanticDocument;
  score: number;
  rankScore: number;
  matchedSignals: string[];
  eventCluster: OntologyEvent | null;
}

export interface OntologyLookup {
  ontology: SemanticOntology;
  themeByAlias: Map<string, OntologyTheme>;
  entityByAlias: Map<string, OntologyEntity>;
  eventById: Map<string, OntologyEvent>;
}

const CASE_SENSITIVE_SHORT_TOKENS = new Set(["US", "UK", "EU", "UN", "AI", "MP", "WHO"]);
const STOP_WORDS = new Set([
  "a",
  "about",
  "after",
  "all",
  "also",
  "an",
  "and",
  "any",
  "are",
  "as",
  "at",
  "be",
  "been",
  "before",
  "between",
  "both",
  "by",
  "can",
  "could",
  "did",
  "do",
  "does",
  "during",
  "for",
  "from",
  "had",
  "has",
  "have",
  "how",
  "in",
  "into",
  "is",
  "it",
  "its",
  "many",
  "more",
  "most",
  "much",
  "news",
  "of",
  "on",
  "or",
  "over",
  "report",
  "reported",
  "reports",
  "said",
  "since",
  "so",
  "some",
  "stories",
  "story",
  "such",
  "than",
  "that",
  "the",
  "their",
  "them",
  "then",
  "there",
  "these",
  "they",
  "this",
  "those",
  "through",
  "to",
  "under",
  "up",
  "was",
  "were",
  "what",
  "when",
  "where",
  "which",
  "while",
  "who",
  "why",
  "with",
  "would",
]);

const MONTHS: Record<string, string> = {
  january: "01",
  jan: "01",
  february: "02",
  feb: "02",
  march: "03",
  mar: "03",
  april: "04",
  apr: "04",
  may: "05",
  june: "06",
  jun: "06",
  july: "07",
  jul: "07",
  august: "08",
  aug: "08",
  september: "09",
  sep: "09",
  sept: "09",
  october: "10",
  oct: "10",
  november: "11",
  nov: "11",
  december: "12",
  dec: "12",
};

const MAX_CLUSTER_DIVERSITY = 4;
const MAX_SEMANTIC_ONTOLOGY_BONUS = 0.06;
const MAX_MATCHED_THEMES = 5;
const MAX_MATCHED_ENTITIES = 5;
const MAX_MATCHED_EVENTS = 5;

function normalizePhrase(value: string): string {
  return value
    .replace(/[_-]+/g, " ")
    .replace(/[^\p{L}\p{N}\s']/gu, " ")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}

function tokenizeWords(value: string): string[] {
  return normalizePhrase(value)
    .split(" ")
    .map((word) => word.replace(/^'+|'+$/g, ""))
    .filter(Boolean);
}

export function buildOntologyLookup(ontology: SemanticOntology | null | undefined): OntologyLookup {
  const safe: SemanticOntology = ontology ?? {
    schemaVersion: 1,
    ontologyVersion: "none",
    themes: [],
    entities: [],
    events: [],
  };
  const themeByAlias = new Map<string, OntologyTheme>();
  const entityByAlias = new Map<string, OntologyEntity>();
  const eventById = new Map<string, OntologyEvent>();

  for (const theme of safe.themes) {
    const candidates = [theme.name, ...(theme.aliases ?? [])];
    for (const candidate of candidates) {
      const key = normalizePhrase(candidate);
      if (!key) continue;
      const existing = themeByAlias.get(key);
      if (!existing || theme.stories > existing.stories) {
        themeByAlias.set(key, theme);
      }
    }
  }

  for (const entity of safe.entities) {
    const candidates = [entity.name, ...(entity.aliases ?? [])];
    for (const candidate of candidates) {
      const key = normalizePhrase(candidate);
      if (!key) continue;
      const existing = entityByAlias.get(key);
      if (!existing || entity.stories > existing.stories) {
        entityByAlias.set(key, entity);
      }
    }
  }

  for (const event of safe.events) {
    eventById.set(event.cluster_id, event);
  }

  return { ontology: safe, themeByAlias, entityByAlias, eventById };
}

export function extractQueryDates(query: string): { years: Set<string>; yearMonths: Set<string> } {
  const years = new Set<string>();
  const yearMonths = new Set<string>();
  const lower = query.toLowerCase();

  for (const match of lower.matchAll(/\b(202[4-9])-(0[1-9]|1[0-2])\b/g)) {
    yearMonths.add(`${match[1]}-${match[2]}`);
    years.add(match[1]);
  }
  const monthNames = Object.keys(MONTHS).join("|");
  const monthYearRegex = new RegExp(`\\b(${monthNames})\\s+(202[4-9])\\b`, "g");
  for (const match of lower.matchAll(monthYearRegex)) {
    const month = MONTHS[match[1]];
    if (month) {
      yearMonths.add(`${match[2]}-${month}`);
      years.add(match[2]);
    }
  }
  for (const match of lower.matchAll(/\b(202[4-9])\b/g)) {
    years.add(match[1]);
  }
  return { years, yearMonths };
}

export function resolveQueryConcepts(
  query: string,
  lookup: OntologyLookup,
): {
  themes: OntologyTheme[];
  entities: OntologyEntity[];
  events: OntologyEvent[];
  years: Set<string>;
  yearMonths: Set<string>;
} {
  const rawTokens = query
    .replace(/[_-]+/g, " ")
    .replace(/[^\p{L}\p{N}\s']/gu, " ")
    .replace(/\s+/g, " ")
    .trim()
    .split(" ")
    .map((word) => word.replace(/^'+|'+$/g, ""))
    .filter(Boolean);
  const lowerTokens = rawTokens.map((token) => token.toLowerCase());
  const usedPositions = new Array<boolean>(lowerTokens.length).fill(false);

  const matchedThemes = new Map<string, OntologyTheme>();
  const matchedEntities = new Map<string, OntologyEntity>();

  // Scan N-grams from longest (4 words) to shortest (1 word) on strict token boundaries.
  // Once a longer phrase matches, mark its token positions as consumed so sub-tokens
  // (e.g. "reform" inside "prison reform" or "labour" inside "labour market") do not
  // trigger unrelated 1-gram entity aliases ("Reform UK", "Labour Party").
  for (let size = Math.min(4, lowerTokens.length); size >= 1; size -= 1) {
    for (let start = 0; start <= lowerTokens.length - size; start += 1) {
      let overlapsConsumed = false;
      for (let pos = start; pos < start + size; pos += 1) {
        if (usedPositions[pos]) {
          overlapsConsumed = true;
          break;
        }
      }
      if (overlapsConsumed) continue;

      const gram = lowerTokens.slice(start, start + size).join(" ");
      const rawGram = rawTokens.slice(start, start + size).join(" ");
      if (size === 1) {
        if (gram.length <= 2) {
          // Short tokens like "us", "in", "un", "uk" must either be uppercase in the query
          // or be an unambiguous multi-letter proper code such as "uk" or "nhs".
          const upper = rawGram.toUpperCase();
          if (
            STOP_WORDS.has(gram) &&
            !(rawGram === upper && CASE_SENSITIVE_SHORT_TOKENS.has(upper))
          ) {
            continue;
          }
          if (gram === "us" && rawGram !== "US" && !/\bu\.s\./i.test(query)) {
            continue;
          }
        } else if (STOP_WORDS.has(gram)) {
          continue;
        }
      }

      const entity = lookup.entityByAlias.get(gram);
      const theme = lookup.themeByAlias.get(gram);
      if (!entity && !theme) continue;

      for (let pos = start; pos < start + size; pos += 1) {
        usedPositions[pos] = true;
      }

      if (entity && !matchedEntities.has(entity.name)) {
        matchedEntities.set(entity.name, entity);
      }
      // If both an entity and a theme have the exact same normalized name (e.g. "Strait of Hormuz"
      // or "Donald Trump"), keep the entity record unless the theme carries settled promotion stats.
      if (theme && !matchedThemes.has(theme.name)) {
        const sameNamedEntity =
          entity && normalizePhrase(entity.name) === normalizePhrase(theme.name);
        if (!sameNamedEntity || theme.promotion !== null) {
          matchedThemes.set(theme.name, theme);
        }
      }
    }
  }

  // Final pass: if a theme has the same normalized name as any matched entity and lacks
  // settled promotion stats, drop the duplicate theme chip.
  const entityNormNames = new Set(
    [...matchedEntities.values()].map((entity) => normalizePhrase(entity.name)),
  );
  for (const [themeName, theme] of [...matchedThemes.entries()]) {
    if (entityNormNames.has(normalizePhrase(themeName)) && theme.promotion === null) {
      matchedThemes.delete(themeName);
    }
  }

  // Also match recurring events whose label shares >= 2 non-stopword tokens with the query.
  const meaningfulQueryTokens = new Set(
    lowerTokens.filter((token) => token.length > 2 && !STOP_WORDS.has(token)),
  );
  const scoredEvents: Array<{ event: OntologyEvent; overlap: number }> = [];
  if (meaningfulQueryTokens.size > 0) {
    for (const event of lookup.ontology.events) {
      const labelTokens = tokenizeWords(event.label).filter(
        (token) => token.length > 2 && !STOP_WORDS.has(token),
      );
      const overlap = labelTokens.filter((token) => meaningfulQueryTokens.has(token)).length;
      if (overlap >= 2 || (overlap === 1 && labelTokens.length === 1)) {
        scoredEvents.push({ event, overlap });
      }
    }
    scoredEvents.sort(
      (left, right) => right.overlap - left.overlap || right.event.article_count - left.event.article_count,
    );
  }

  const { years, yearMonths } = extractQueryDates(query);
  return {
    themes: [...matchedThemes.values()].sort((a, b) => b.stories - a.stories),
    entities: [...matchedEntities.values()].sort((a, b) => b.stories - a.stories),
    events: scoredEvents.slice(0, MAX_MATCHED_EVENTS).map((item) => item.event),
    years,
    yearMonths,
  };
}

export function matchesFilters(document: SemanticDocument, filters: SearchFilters): boolean {
  return (
    (!filters.surface ||
      filters.surface === "all" ||
      document.surfaces.includes(filters.surface)) &&
    (!filters.topic || filters.topic === "all" || document.topic === filters.topic) &&
    (!filters.storyForm ||
      filters.storyForm === "all" ||
      document.story_form === filters.storyForm)
  );
}

export function lexicalScore(query: string, document: SemanticDocument): number {
  const terms = tokenizeWords(query).filter((term) => !STOP_WORDS.has(term) || term.length > 2);
  const activeTerms = terms.length ? terms : tokenizeWords(query);
  if (!activeTerms.length) return 0;

  const titleWords = new Set(tokenizeWords(document.title));
  const bodyWords = new Set([
    ...titleWords,
    ...tokenizeWords(document.summary),
    ...document.themes.flatMap((theme) => tokenizeWords(theme)),
    ...document.named_entities.flatMap((entity) => tokenizeWords(entity)),
  ]);

  let matches = 0;
  let titleMatches = 0;
  for (const term of activeTerms) {
    if (bodyWords.has(term)) matches += 1;
    if (titleWords.has(term)) titleMatches += 1;
  }
  return (matches + titleMatches * 1.5) / (activeTerms.length * 2.5);
}

export function rankSearchDocuments(options: {
  query: string;
  documents: SemanticDocument[];
  filters: SearchFilters;
  lookup: OntologyLookup;
  semanticScoreAt?: (index: number) => number;
  limit?: number;
}): {
  results: SearchResultItem[];
  ontologyContext: OntologyContext;
} {
  const { query, documents, filters, lookup, semanticScoreAt, limit = 20 } = options;
  const resolved = resolveQueryConcepts(query, lookup);
  const resolvedThemeNames = new Set(resolved.themes.map((theme) => theme.name.toLowerCase()));
  const resolvedEntityNames = new Set(resolved.entities.map((entity) => entity.name.toLowerCase()));
  const resolvedEventIds = new Set(resolved.events.map((event) => event.cluster_id));

  const themeFilterCounts = new Map<string, number>();
  const entityFilterCounts = new Map<string, number>();
  let matchedConceptStoryCount = 0;

  const candidates: Array<{
    document: SemanticDocument;
    score: number;
    rankScore: number;
    matchedSignals: string[];
    eventCluster: OntologyEvent | null;
  }> = [];

  for (let index = 0; index < documents.length; index += 1) {
    const document = documents[index];
    if (!matchesFilters(document, filters)) continue;

    const docThemesLower = document.themes.map((theme) => theme.toLowerCase());
    const docEntitiesLower = document.named_entities.map((entity) => entity.toLowerCase());

    let matchedAnyConcept = false;
    const matchedSignals: string[] = [];

    for (let t = 0; t < document.themes.length; t += 1) {
      const lower = docThemesLower[t];
      if (resolvedThemeNames.has(lower)) {
        matchedAnyConcept = true;
        matchedSignals.push(`Theme: ${document.themes[t]}`);
        themeFilterCounts.set(lower, (themeFilterCounts.get(lower) ?? 0) + 1);
      }
    }

    for (let e = 0; e < document.named_entities.length; e += 1) {
      const lower = docEntitiesLower[e];
      if (resolvedEntityNames.has(lower)) {
        matchedAnyConcept = true;
        matchedSignals.push(`Entity: ${document.named_entities[e]}`);
        entityFilterCounts.set(lower, (entityFilterCounts.get(lower) ?? 0) + 1);
      }
    }

    const eventCluster = document.cluster_id
      ? (lookup.eventById.get(document.cluster_id) ?? null)
      : typeof document.cluster_index === "number" &&
          document.cluster_index >= 0 &&
          document.cluster_index < lookup.ontology.events.length
        ? lookup.ontology.events[document.cluster_index]
        : null;

    if (eventCluster && resolvedEventIds.has(eventCluster.cluster_id)) {
      matchedAnyConcept = true;
      matchedSignals.push(`Event: ${eventCluster.label}`);
    }

    const dateKey = (document.first_seen || document.fetched_at || "").slice(0, 10);
    const yearMonth = dateKey.slice(0, 7);
    const year = dateKey.slice(0, 4);
    let dateMatched = false;
    if (resolved.yearMonths.size > 0 && resolved.yearMonths.has(yearMonth)) {
      dateMatched = true;
      matchedSignals.push(`Period: ${yearMonth}`);
    } else if (
      resolved.yearMonths.size === 0 &&
      resolved.years.size > 0 &&
      resolved.years.has(year)
    ) {
      dateMatched = true;
      matchedSignals.push(`Year: ${year}`);
    }

    if (matchedAnyConcept) {
      matchedConceptStoryCount += 1;
    }

    const lex = lexicalScore(query, document);
    const rawScore = semanticScoreAt ? semanticScoreAt(index) : lex;
    const score = Math.max(-1, Math.min(1, rawScore));

    let bonus = 0;
    if (semanticScoreAt) {
      const entityHit = matchedSignals.some((item) => item.startsWith("Entity:")) ? 0.025 : 0;
      const themeHit = matchedSignals.some((item) => item.startsWith("Theme:")) ? 0.02 : 0;
      const eventHit = matchedSignals.some((item) => item.startsWith("Event:")) ? 0.015 : 0;
      const dateHit = dateMatched ? 0.01 : 0;
      const lexHit = lex * 0.02;
      bonus = Math.min(
        MAX_SEMANTIC_ONTOLOGY_BONUS,
        entityHit + themeHit + eventHit + dateHit + lexHit,
      );
    } else {
      const conceptBonus = matchedSignals.length * 0.15 + (dateMatched ? 0.1 : 0);
      bonus = conceptBonus;
    }

    const rankScore = semanticScoreAt ? score + bonus : score + bonus;
    if (rankScore <= 0) continue;

    candidates.push({
      document,
      score: semanticScoreAt ? score : Math.min(1, rankScore),
      rankScore,
      matchedSignals: matchedSignals.slice(0, 4),
      eventCluster,
    });
  }

  candidates.sort((left, right) => right.rankScore - left.rankScore || right.score - left.score);

  // Apply per-cluster diversity cap so a single continuing story does not crowd out all 20 slots.
  const selected: SearchResultItem[] = [];
  const overflow: SearchResultItem[] = [];
  const clusterCounts = new Map<string, number>();

  for (const candidate of candidates) {
    if (selected.length >= limit) break;
    const clusterId = candidate.eventCluster?.cluster_id;
    if (clusterId) {
      const count = clusterCounts.get(clusterId) ?? 0;
      if (count >= MAX_CLUSTER_DIVERSITY) {
        overflow.push(candidate);
        continue;
      }
      clusterCounts.set(clusterId, count + 1);
    }
    selected.push(candidate);
  }

  for (const candidate of overflow) {
    if (selected.length >= limit) break;
    selected.push(candidate);
  }

  // Also surface recurring events and top themes/entities represented in the selected results.
  const topThemes = [...resolved.themes];
  const topEntities = [...resolved.entities];
  const topEvents = [...resolved.events];
  const seenEventIds = new Set(topEvents.map((event) => event.cluster_id));

  for (const item of selected) {
    if (
      item.eventCluster &&
      !seenEventIds.has(item.eventCluster.cluster_id) &&
      topEvents.length < MAX_MATCHED_EVENTS
    ) {
      seenEventIds.add(item.eventCluster.cluster_id);
      topEvents.push(item.eventCluster);
    }
  }

  const themesWithIds: MatchedThemeRecord[] = topThemes
    .slice(0, MAX_MATCHED_THEMES)
    .map((theme, idx) => ({
      ...theme,
      id: `T${idx + 1}`,
      filter_matches: themeFilterCounts.get(theme.name.toLowerCase()) ?? theme.stories,
    }));

  const entitiesWithIds: MatchedEntityRecord[] = topEntities
    .slice(0, MAX_MATCHED_ENTITIES)
    .map((entity, idx) => ({
      ...entity,
      id: `E${idx + 1}`,
      filter_matches: entityFilterCounts.get(entity.name.toLowerCase()) ?? entity.stories,
    }));

  const eventsWithIds: MatchedEventRecord[] = topEvents
    .slice(0, MAX_MATCHED_EVENTS)
    .map((event, idx) => ({
      ...event,
      id: `R${idx + 1}`,
    }));

  return {
    results: selected,
    ontologyContext: {
      ontologyVersion: lookup.ontology.ontologyVersion,
      activeFilters: filters,
      matchedConceptStoryCount,
      themes: themesWithIds,
      entities: entitiesWithIds,
      events: eventsWithIds,
    },
  };
}

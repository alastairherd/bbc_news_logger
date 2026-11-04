export interface CitationSource {
  id: string;
  title: string;
  href: string;
  dateLabel?: string;
}

type ResolveSource = (id: string) => CitationSource | undefined;

const token = "(?:\\d+|[TER]\\d+)";
const marker = `\\[${token}(?:\\s*,\\s*${token})*\\]`;
let nextGroup = 0;

function escape(value: string): string {
  const entities: Record<string, string> = { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" };
  return value.replace(/[&<>"']/g, (character) => entities[character]!);
}

function sourceLabel(source: CitationSource): string {
  return `Source ${source.id}: ${source.title}${source.dateLabel ? ` (${source.dateLabel})` : ""}`;
}

/** Keep reference controls inline, with their block-level evidence lists after the paragraph. */
export function createCitationRenderer(resolve: ResolveSource) {
  const panels: string[] = [];

  function group(ids: Array<string | number>): string {
    const entries = new Map<string, { source: CitationSource; ids: string[] }>();
    const unknown: string[] = [];
    for (const id of new Set(ids.map(String))) {
      const source = resolve(id);
      if (!source) {
        unknown.push(`[${escape(id)}]`);
        continue;
      }
      const existing = entries.get(source.href);
      if (existing) existing.ids.push(id);
      else entries.set(source.href, { source, ids: [id] });
    }
    const sources = [...entries.values()];
    const unresolved = unknown.join(" ");
    if (!sources.length) return unresolved;
    if (sources.length === 1 && sources[0]!.ids.length === 1) {
      const { source } = sources[0]!;
      const label = escape(sourceLabel(source));
      return `<a class="citation citation-link" href="${escape(source.href)}" aria-label="${label}" title="${label}">[${escape(source.id)}]</a>${unresolved}`;
    }

    const id = `citation-group-${++nextGroup}`;
    const count = `${sources.length} ${sources.length === 1 ? "source" : "sources"}`;
    panels.push(`<div class="citation-panel" id="${id}" role="region" aria-labelledby="${id}-toggle" hidden>
      <p class="citation-panel-heading">Supporting sources</p>
      <ul class="citation-source-list">${sources.map(({ source, ids }) => `<li>
        <a href="${escape(source.href)}" aria-label="${escape(`Source ${ids.join(", ")}: ${source.title}${source.dateLabel ? ` (${source.dateLabel})` : ""}`)}">${escape(source.title)}</a>
        <span class="citation-source-meta">${escape(`${ids.length === 1 ? "Reference" : "References"} ${ids.join(", ")}${source.dateLabel ? ` · ${source.dateLabel}` : ""}`)}</span>
      </li>`).join("")}</ul>
    </div>`);
    return `<button class="citation citation-toggle" type="button" id="${id}-toggle" aria-controls="${id}" aria-expanded="false" aria-label="Show ${count} for this claim" data-citation-count="${count}">[${count}]</button>${unresolved}`;
  }

  function text(value: string): string {
    const normalized = value.replace(/\(([TER]\d+)\)/g, "[$1]");
    const clusters = new RegExp(`${marker}(?:\\s*${marker})*`, "g");
    let html = "";
    let cursor = 0;
    for (const match of normalized.matchAll(clusters)) {
      html += escape(normalized.slice(cursor, match.index));
      html += group(match[0].match(/[TER]\d+|\d+/g) ?? []);
      cursor = match.index + match[0].length;
    }
    return html + escape(normalized.slice(cursor));
  }

  return { text, group, panels: () => panels.join("") };
}

/** Delegation also covers answers replaced by a later search or retry. */
export function bindCitationDisclosures(container: HTMLElement): void {
  function toggle(button: HTMLButtonElement, expanded: boolean) {
    const panel = document.getElementById(button.getAttribute("aria-controls") ?? "");
    if (!panel || !container.contains(panel)) return;
    panel.hidden = !expanded;
    button.setAttribute("aria-expanded", String(expanded));
    button.setAttribute("aria-label", `${expanded ? "Hide" : "Show"} ${button.dataset.citationCount} for this claim`);
  }

  container.addEventListener("click", (event) => {
    const button = event.target instanceof Element ? event.target.closest<HTMLButtonElement>(".citation-toggle") : null;
    if (button && container.contains(button)) toggle(button, button.getAttribute("aria-expanded") !== "true");
  });
  container.addEventListener("keydown", (event) => {
    if (event.key !== "Escape" || !(event.target instanceof Element)) return;
    const panel = event.target.closest<HTMLElement>(".citation-panel");
    const button = panel
      ? document.getElementById(`${panel.id}-toggle`) as HTMLButtonElement | null
      : event.target.closest<HTMLButtonElement>(".citation-toggle");
    if (button?.getAttribute("aria-expanded") === "true") {
      toggle(button, false);
      button.focus();
      event.preventDefault();
    }
  });
}

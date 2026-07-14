import { readFile } from "node:fs/promises";

const base = "/bbc_news_logger/";
const pages = ["index.html", "explore/index.html", "signals/index.html", "methodology/index.html"];
const output = await Promise.all(
  pages.map(async (page) => [page, await readFile(new URL(`../dist/${page}`, import.meta.url), "utf8")]),
);

for (const [page, html] of output) {
  for (const brokenPath of [
    "/bbc_news_loggerexplore",
    "/bbc_news_loggersignals",
    "/bbc_news_loggermethodology",
    "/bbc_news_loggerdata",
  ]) {
    if (html.includes(brokenPath)) {
      throw new Error(`${page} contains malformed GitHub Pages path: ${brokenPath}`);
    }
  }

  for (const route of [base, `${base}explore/`, `${base}signals/`, `${base}methodology/`]) {
    if (!html.includes(`href="${route}"`)) {
      throw new Error(`${page} is missing route link: ${route}`);
    }
  }
}

const index = output.find(([page]) => page === "index.html")?.[1] ?? "";
const explore = output.find(([page]) => page === "explore/index.html")?.[1] ?? "";
const signals = output.find(([page]) => page === "signals/index.html")?.[1] ?? "";
async function routeScripts(html) {
  return Promise.all(
    [...html.matchAll(/src="(\/bbc_news_logger\/[^\"]+\.js)"/g)].map((match) =>
    readFile(new URL(`../dist/${match[1].slice(base.length)}`, import.meta.url), "utf8"),
    ),
  );
}
const indexScripts = await routeScripts(index);
const exploreScripts = await routeScripts(explore);
const exploreOutput = `${explore}\n${exploreScripts.join("\n")}`;
const signalScripts = await routeScripts(signals);
const signalOutput = `${signals}\n${signalScripts.join("\n")}`;
const indexOutput = `${index}\n${indexScripts.join("\n")}`;

for (const mart of ["data/manifest.json", "data/daily.json"]) {
  if (!indexOutput.includes(mart)) throw new Error(`index route is missing mart request: ${mart}`);
}

for (const mart of ["data/stories.json", "data/rank-series.json"]) {
  if (!exploreOutput.includes(mart)) throw new Error(`explore route is missing mart request: ${mart}`);
}

for (const mart of [
  "data/semantic-trends.json",
  "data/recurring-events.json",
  "data/semantic-findings.json",
  "data/semantic-documents.json",
  "data/semantic-vectors.i8",
]) {
  if (!signalOutput.includes(mart)) throw new Error(`signals route is missing mart request: ${mart}`);
}

console.log(`Verified ${pages.length} GitHub Pages routes and nine data-mart requests.`);

import { readFile } from "node:fs/promises";

const base = "/bbc_news_logger/";
const pages = ["index.html", "explore/index.html", "methodology/index.html"];
const output = await Promise.all(
  pages.map(async (page) => [page, await readFile(new URL(`../dist/${page}`, import.meta.url), "utf8")]),
);

for (const [page, html] of output) {
  for (const brokenPath of [
    "/bbc_news_loggerexplore",
    "/bbc_news_loggermethodology",
    "/bbc_news_loggerdata",
  ]) {
    if (html.includes(brokenPath)) {
      throw new Error(`${page} contains malformed GitHub Pages path: ${brokenPath}`);
    }
  }

  for (const route of [base, `${base}explore/`, `${base}methodology/`]) {
    if (!html.includes(`href="${route}"`)) {
      throw new Error(`${page} is missing route link: ${route}`);
    }
  }
}

const index = output.find(([page]) => page === "index.html")?.[1] ?? "";
const explore = output.find(([page]) => page === "explore/index.html")?.[1] ?? "";

for (const mart of ["data/manifest.json", "data/daily.json"]) {
  if (!index.includes(mart)) throw new Error(`index.html is missing mart request: ${mart}`);
}

for (const mart of ["data/stories.json", "data/rank-series.json"]) {
  if (!explore.includes(mart)) throw new Error(`explore/index.html is missing mart request: ${mart}`);
}

console.log(`Verified ${pages.length} GitHub Pages routes and four data-mart requests.`);

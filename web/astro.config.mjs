import { defineConfig } from "astro/config";

export default defineConfig({
  site: "https://alastairherd.github.io",
  base: "/bbc_news_logger",
  output: "static",
  build: { format: "directory" },
});

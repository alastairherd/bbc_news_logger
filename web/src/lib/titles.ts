// Live pages are marked by their URL. Titles captured before the 28 October 2026 scraper fix
// also carry a "Live. " prefix, sometimes with a mis-decoded non-breaking space ("Â").
const LIVE_PREFIX = /^live\s*[.:·\-– Â\s]+/i;

// Older captures decoded UTF-8 as Latin-1, so "£" became "Â£". Each misread character is a
// UTF-8 lead byte followed by continuation bytes, which decode back exactly when valid.
const MISREAD_UTF8 = /[Â-ô][\u0080-¿]{1,3}/g;
const utf8 = new TextDecoder("utf-8", { fatal: true });

export function repairMisreadText(text: string): string {
  return text.replace(MISREAD_UTF8, (run) => {
    try {
      return utf8.decode(Uint8Array.from(run, (character) => character.charCodeAt(0)));
    } catch {
      return run;
    }
  });
}

export function titleParts(title: string, url: string): { live: boolean; text: string } {
  const repaired = repairMisreadText(title);
  const match = repaired.match(LIVE_PREFIX);
  return { live: Boolean(match) || /\/news\/live\//.test(url), text: match ? repaired.slice(match[0].length) : repaired };
}

---
name: BBC News Analyser
description: An independent archive of the BBC News front page and Most Read list, set in the reading patterns of the page it records.
colors:
  bbc-news-red: "oklch(0.491 0.202 29)"
  red-deep: "oklch(0.408 0.168 29)"
  red-wash: "oklch(0.968 0.012 29)"
  comparison-blue: "oklch(0.526 0.143 248)"
  status-teal: "oklch(0.609 0.107 187)"
  success-green: "oklch(0.495 0.112 155)"
  sulphur-highlight: "oklch(0.94 0.055 97)"
  paper: "oklch(1 0 0)"
  surface-grey: "oklch(0.973 0 0)"
  ink: "oklch(0.191 0 0)"
  muted-ink: "oklch(0.452 0.004 248)"
  rule: "oklch(0.898 0 0)"
  rule-strong: "oklch(0.8 0 0)"
typography:
  display:
    fontFamily: "Inter Variable, Inter, ui-sans-serif, system-ui, sans-serif"
    fontSize: "2.5rem"
    fontWeight: 700
    lineHeight: 1.08
    letterSpacing: "-0.025em"
  headline:
    fontFamily: "Inter Variable, Inter, ui-sans-serif, system-ui, sans-serif"
    fontSize: "1.5rem"
    fontWeight: 700
    lineHeight: 1.15
    letterSpacing: "-0.02em"
  section-title:
    fontFamily: "Inter Variable, Inter, ui-sans-serif, system-ui, sans-serif"
    fontSize: "1.25rem"
    fontWeight: 700
    lineHeight: 1.15
    letterSpacing: "-0.01em"
  title:
    fontFamily: "Inter Variable, Inter, ui-sans-serif, system-ui, sans-serif"
    fontSize: "1.0625rem"
    fontWeight: 700
    lineHeight: 1.25
    letterSpacing: "-0.015em"
  story:
    fontFamily: "Inter Variable, Inter, ui-sans-serif, system-ui, sans-serif"
    fontSize: "1rem"
    fontWeight: 700
    lineHeight: 1.3
    letterSpacing: "-0.015em"
  body:
    fontFamily: "Inter Variable, Inter, ui-sans-serif, system-ui, sans-serif"
    fontSize: "1rem"
    fontWeight: 400
    lineHeight: 1.5
  label:
    fontFamily: "Inter Variable, Inter, ui-sans-serif, system-ui, sans-serif"
    fontSize: "0.8125rem"
    fontWeight: 600
    lineHeight: 1.3
    fontFeature: "\"tnum\" 1"
  rank-numeral:
    fontFamily: "Inter Variable, Inter, ui-sans-serif, system-ui, sans-serif"
    fontSize: "2.25rem"
    fontWeight: 300
    lineHeight: 0.95
    letterSpacing: "-0.04em"
    fontFeature: "\"tnum\" 1"
  wordmark:
    fontFamily: "Inter Variable, Inter, ui-sans-serif, system-ui, sans-serif"
    fontSize: "1.5rem"
    fontWeight: 800
    lineHeight: 1
    letterSpacing: "-0.035em"
rounded:
  none: "0px"
  dot: "50%"
spacing:
  hair: "4px"
  xs: "8px"
  row: "14px"
  sm: "16px"
  md: "20px"
  gutter: "24px"
  column: "40px"
  section: "56px"
components:
  section-nav-link:
    textColor: "{colors.ink}"
    typography: "{typography.label}"
    padding: "12px 16px"
  section-nav-link-active:
    textColor: "{colors.ink}"
  rank-numeral:
    textColor: "{colors.bbc-news-red}"
    typography: "{typography.rank-numeral}"
    width: "2.75rem"
  marker-new:
    textColor: "{colors.bbc-news-red}"
    typography: "{typography.label}"
  marker-also:
    textColor: "{colors.comparison-blue}"
    typography: "{typography.label}"
  marker-move:
    textColor: "{colors.ink}"
    typography: "{typography.label}"
  action-link:
    textColor: "{colors.bbc-news-red}"
    typography: "{typography.label}"
  action-link-hover:
    textColor: "{colors.red-deep}"
  status-dot:
    backgroundColor: "{colors.status-teal}"
    rounded: "{rounded.dot}"
    size: "8px"
  error-panel:
    backgroundColor: "{colors.red-wash}"
    textColor: "{colors.ink}"
    rounded: "{rounded.none}"
    padding: "14px 16px"
---

# Design System: BBC News Analyser

## Overview

**Creative North Star: "The Front Page, Annotated"**

The archive sets its latest capture as the front page it recorded and writes its own measurements into the margins. The reading pattern is the BBC News website's: a white page, near-black bold headlines, a lead story over a four-up grid and compact rows, a Most Read column of large red numerals, and hairline rules doing all the structural work. What makes it an instrument rather than a copy is the annotation layer: position numbers, "New", "Up 3", "Also #7 in Front page", capture times in UTC, collection status, and a chart that marks its own gaps.

Density is newspaper-high and calm. Nothing floats: there are no cards, no shadows, no rounded panels, no fluid display type. Hierarchy comes from weight, size, and a single red accent used for rank, the active section, freshness, and the next action. The archive keeps its own text wordmark and says in the header that it is independent, so it reads as a study of BBC News, never as BBC News.

The Overview page and the shared shell (masthead, section bar, footer) are the system. Explore, Ask & analyse, and Methodology still run on the previous data-lab styling and are legacy pending redesign; nothing on them is part of this system.

**Key Characteristics:**
- White canvas, near-black ink, grey hairline rules; BBC News red as the only accent.
- Headline-led hierarchy in Inter: bold, tight, left-aligned, tabular figures for every number.
- Structure by rules and columns, never by cards or elevation.
- An annotation vocabulary (position, movement, "New", cross-surface "Also") set in small meta text beside each headline.
- Independence stated in the masthead; no BBC marks.

## Colors

A newspaper palette: white paper, near-black ink, grey rules, one red, and a small set of Sanzo Wada partners for the jobs red cannot do.

### Primary
- **BBC News Red** (#B80000, chosen by the user; 6.9:1 on white): rank numerals in Most Read, the active-section underline, the "New" marker and key entry, the "Live" label on live headlines, action links ("Inspect trends", "Show daily figures as a table"), the Most Read chart series and lean, focus outlines, form accent colour, and error and stale states.
- **Red Deep** (#8F0000): hover state for red action links.
- **Red Wash**: the pale fill of the error panel only.

### Secondary
- **Comparison Blue** (Wada blue): the cross-surface "Also #n in …" marker and its key entry. It is the third series whenever a chart needs one beyond ink and red; it stays distinct from red under protanopia, deuteranopia, and tritanopia.

### Tertiary
- **Status Teal** (Wada benzol green; 3.6:1 on white): the "current" collection status dot. Graphics only.
- **Success Green** (Wada diamine green): reserved for labelled positive text and icons. Defined on the root; no Overview use yet.
- **Sulphur Highlight** (Wada sulphur yellow): text selection, with ink on top; reserved for matched search terms when Explore is redesigned.

The supporting colours come from Sanzo Wada's *A Dictionary of Color Combinations*, partnered with carmine, the closest Wada colour to BBC News red: black (combination 117), blue (154 and 257), diamine green (313), benzol green (122 and 266), and sulphur yellow (246, with brick red).

### Neutral
- **Paper** (white): the page, masthead, and section bar.
- **Surface Grey** (#F6F6F6): loading skeleton bars and the "no collection" band in charts. Never a card fill.
- **Ink** (#141414): headlines, body, the front-page chart series, the heavy rule above section headings and the footer.
- **Muted Ink** (#545658): meta text, positions, captions, axis labels, column notes.
- **Rule** (#DDDDDD): every divider between stories, columns, status cells, and table rows.
- **Rule Strong** (#BEBEBE): the separators inside story meta, chart ticks, the section-nav hover underline.

### Named Rules
**The One Red Rule.** Red is the only accent and it always means something: rank, current section, new, live, an action, or a fault. It never fills a header band, a panel background, or a button block.

**The Never Red-and-Green Rule.** Red and green are never paired as chart series. The front page is ink and Most Read is red; the lightness gap carries the distinction, and blue is the third series.

**The Teal Is Not Text Rule.** Status teal marks dots and icons only. It is never used for text.

**The Not By Colour Alone Rule.** Errors and states share the red family, so they always carry a second signal: the error panel has an alert icon and a bold "Error:" label; stale and failed status read in words beside the dot; delayed uses a hollow ring rather than a hue; movement markers carry chevrons and the words "Up" or "Down".

## Typography

**Display Font:** Inter Variable, self-hosted (with ui-sans-serif, system-ui, Segoe UI, sans-serif)
**Body Font:** the same Inter stack
**Label/Mono Font:** none on the system surface; a ui-monospace stack exists for identifiers and code only

**Character:** One sans family carried entirely by weight and size, as a news front page is: heavy, tightly led headlines over plain reading text, with tabular figures so ranks, counts, and times align.

### Hierarchy
- **Display** (700, 2.5rem, 1.08): the lead story headline only. Steps to 2.125rem below 1100px and 1.75rem below 760px.
- **Headline** (700, 1.5rem, 1.15): page heading ("Latest capture", 1.375rem on mobile) and the lead theme in What's changing.
- **Section Title** (700, 1.25rem): section headings, always sitting on a 1px ink rule.
- **Title** (700, 1.0625rem, 1.25): four-up grid headlines and column headings.
- **Story** (700, 1rem, 1.3): compact rows and Most Read headlines.
- **Body** (400, 1rem, 1.5): running text, capped at 70ch; supporting intros at 0.9375rem in muted ink.
- **Label** (400 or 600, 0.8125rem, 1.3): story meta, status bar, captions, keys, action links. Section nav uses 600 at 0.9375rem.
- **Rank Numeral** (300, 2.25rem, 0.95, tabular): Most Read ranks in red. The light weight against bold headlines is deliberate, as on BBC News.
- **Wordmark** (800, 1.5rem, -0.035em): "BBC News Analyser" set as text.

### Named Rules
**The Tabular Figures Rule.** Every rank, count, percentage-point figure, time, and chart label uses tabular numerals.

**The No Mono Rule.** Monospace is for identifiers and code only, never for labels, timestamps, or metadata.

**The Fixed Scale Rule.** Sizes are fixed rem steps changed at breakpoints; no clamp-based fluid display type.

## Layout

Content sits in one measure of 80rem with 24px gutters (16px below 760px). The masthead bar holds the wordmark, the independence note, and a right-aligned Dataset link; the section nav sits beneath it, separated by a rule. A slim status bar of rule-separated cells follows (capture time, collection state, then counts that hide on mobile).

The capture area is a two-thirds / one-third grid with 40px of padding either side of a vertical rule: the front page on the left (lead story, a four-up grid with vertical rules between items, then compact rows six to ten in two columns), Most Read on the right. Section intros use a one-third / two-thirds split with the heading left and the muted description right. What's changing is three rule-separated columns. Sections are 56px apart; story rows are 14px top and bottom with a rule beneath.

Breakpoints: below 1100px the four-up grid becomes two-up and the lead steps down; below 900px the capture grid, section intros, and columns collapse to one reading order, and Most Read follows the front page; below 760px gutters tighten, the masthead note wraps under the wordmark, secondary status cells hide, and compact rows go single-column; below 520px the grid is a single column with horizontal rules.

## Elevation & Depth

Flat. Nothing on the system surface casts a shadow or sits on a raised card; depth is replaced by rules (1px grey between items, 1px ink above sections and the footer) and by column structure. The only box-shadows in use are inset strokes that draw lines: the 4px red underline for the active section, the 3px grey hover underline, and the hollow ring of the "delayed" status dot.

### Named Rules
**The Rules Not Cards Rule.** Group content with hairline rules and columns. If a container seems to need a border on all four sides, a background, or a shadow, it is the wrong container for this world. The error panel is the single bordered box, because it is an alert.

## Shapes

Square everywhere: no radius on panels, skeletons, links, or tables. The only curve is the 8px circular status dot. Lines are the form language: 1px rules, a 4px underline for the current section, 2px chart lines with round joins, and 1.6px-stroke line icons (external arrow, up and down chevrons, alert circle, warning triangle) drawn inline as SVG at 1em.

## Components

### Navigation
- **Style:** a white bar under the masthead, items in ink at 0.9375rem semibold, 12px by 16px padding, the first item flush left; scrolls horizontally on narrow screens without a scrollbar.
- **States:** hover adds a 3px grey underline; the current section has a 4px red underline; transition 180ms ease-out.
- **Mobile:** items tighten to 0.8125rem with 8px side padding.

### Lead story
The page's single display moment: the position number in muted label text, then the headline at display size, an optional red uppercase "Live" label in 800 weight leading the headline, then the story meta row. Headlines link to the BBC article and underline on hover.

### Story meta and markers
A row of small muted items separated by short vertical rules (the rule is clipped so no line ever starts with a divider). Markers: "New" in bold red; "Up n" / "Down n" in ink with a chevron icon; "Also #n in Front page / Most Read" in semibold blue; then plain muted context ("First seen this capture", "Tracked 7 hrs"). In the four-up grid the meta stacks vertically without dividers.

### Most Read ranked list
Each row is a 2.75rem numeral column and the headline with its meta, a rule beneath. The numeral is red, light weight, large, and tabular.

### Section heading
Bold title on a 1px ink rule. In section intros the description sits to the right in muted body text; any caution appears beneath it with a warning-triangle icon in semibold ink.

### Change lists
Rows of a semibold link and a right-aligned muted tabular figure, rule beneath. A figure that leans to Most Read is red; one that leans to the front page is ink. Column headings carry a red action link on the right.

### Status bar
Rule-separated cells of muted term and semibold value. Collection state pairs a dot with words: teal dot for current, hollow ring for delayed, red dot and red text for stale or failed.

### Error panel
1px red border, red-wash fill, alert-circle icon in red, and a bold red "Error:" lead-in before ink text.

### Chart
Line chart with ink for front page and red for Most Read, dashed lines for reduced-rate periods, a surface-grey band labelled for no-collection gaps, grey gridlines, muted 12px tabular axis text, and a legend of drawn line swatches. Every chart has a caption with its range and a red "Show … as a table" disclosure opening a semantic table.

### Loading
Skeletons are surface-grey bars shaped like the content they replace (lead lines, four grid blocks, list rows), so layout never jumps.

## Do's and Don'ts

### Do:
- **Do** use BBC News red (#B80000) for rank numerals, the active-section underline, "New", "Live", action links, focus, and faults, and nothing else.
- **Do** separate stories, columns, and status cells with 1px grey rules, and put a 1px ink rule above every section heading and the footer.
- **Do** set every number in tabular figures.
- **Do** pair every state colour with words or an icon: "Error:" and an alert icon, "Delayed" with a hollow ring, chevrons with "Up" and "Down".
- **Do** state independence in the header ("Independent archive · not affiliated with the BBC") and repeat it in the footer.
- **Do** give every chart a caption, a legend, marked gaps, and a table alternative.
- **Do** honour reduced motion; the only transition is the 180ms ease-out nav underline.

### Don't:
- **Don't** pair red and green as chart series.
- **Don't** use status teal for text.
- **Don't** let an error or state rely on colour alone.
- **Don't** use the BBC blocks logo, a letter-in-square mark, the Reith typeface, or a full-width red header band.
- **Don't** use cards, drop shadows, or rounded panels to group content.
- **Don't** set labels, timestamps, or metadata in monospace.
- **Don't** add uppercase kickers or eyebrows above headings; the only uppercase label is "Live", and it appears only when the story is live.
- **Don't** carry the legacy navy, cyan, amber, or dark-panel styling from Explore, Ask & analyse, or Methodology onto new surfaces.

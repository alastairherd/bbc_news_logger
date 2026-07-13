# Design System

## Direction

A restrained technical data-lab interface for long desktop research sessions in ordinary office light, with structural mobile adaptations. It is light-first for chart legibility and uses a dark navy analysis band only where focus benefits from stronger contrast.

## Color

Use OKLCH tokens throughout:

- Canvas: `oklch(0.975 0.006 245)`
- Surface: `oklch(0.995 0.002 245)`
- Surface muted: `oklch(0.94 0.012 245)`
- Ink: `oklch(0.23 0.035 252)`
- Muted ink: `oklch(0.43 0.025 252)`
- Rule: `oklch(0.84 0.018 245)`
- Primary: `oklch(0.49 0.14 252)`
- Primary hover: `oklch(0.42 0.15 252)`
- Signal cyan: `oklch(0.72 0.13 205)`
- Signal amber: `oklch(0.76 0.14 78)`
- Error: `oklch(0.55 0.19 25)`
- Success: `oklch(0.55 0.12 155)`

Primary marks selection and navigation. Cyan and amber distinguish front-page and most-read series. They are not decorative accents.

## Typography

Use Inter or the system sans stack for UI and prose. Use IBM Plex Mono or the system monospace stack only for timestamps, identifiers, ranks, and code. Use a compact fixed scale from 0.75rem to 2.25rem with strong weight contrast and no fluid display typography.

## Layout

- Maximum content width: 1440px with 24px desktop and 16px mobile gutters.
- Sticky filter bar below the project header.
- Two-column analysis layouts collapse to a single reading order below 900px.
- Tables scroll horizontally only when a stacked row representation would lose meaning.
- Story detail expands inline and is encoded in the URL rather than opening a modal.

## Components

- Controls use 8px radii, clear labels, and visible default, hover, focus, active, disabled, loading, and error states.
- Analytical sections use rules and spacing rather than repeated floating cards.
- Charts always include units, source notes, keyboard-accessible focus targets, and a table alternative.
- Skeletons preserve layout during data loading; empty states explain how to broaden the current filters.
- Status text distinguishes current, delayed, stale, waking, quota-limited, and failed states.

## Motion

Use 150–220ms ease-out transitions only for filter feedback, inline expansion, and focus changes. Respect `prefers-reduced-motion` and never gate content visibility on animation.

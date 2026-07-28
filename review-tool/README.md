# MeTTa Review Tool

Standalone Next.js app for human verification of NL &rarr; MeTTa translation pairs
(the SNLI-Silver dataset). For each pair a reviewer judges, independently:

- **premise_ok** — do the MeTTa atoms faithfully encode the English premise?
- **hypothesis_ok** — do the MeTTa atoms faithfully encode the English hypothesis?

Both Y &rarr; **Correct** + **Final**. Either N &rarr; **Failed** (with optional note),
fixable on a separate page &rarr; **Fixed** + **Final**. Old/new UUIDs tracked throughout.

## Stack

Next.js 16 (App Router) · React 19 · TypeScript · Prisma 7 + Postgres · Stack Auth ·
Tailwind v4 · Recharts.

## Data model

`Annotation` (source) &rarr; `Correct` / `Failed` &rarr; `Fixed` &rarr; `Final`, linked by UUID PK/FK.

## Setup

```bash
npm install
cp .env.example .env.local          # fill DATABASE_URL (Neon) + Stack Auth keys
npm run db:generate                 # generate Prisma client
npm run db:migrate -- --name init   # create tables (needs DATABASE_URL)
npm run import                      # load annotations.json -> Annotation table
npm run dev                         # http://localhost:3000
```

Dry-run the importer without a database:

```bash
npm run import -- --dry-run
```

By default the importer reads `../SNLI-Silver-Dataset/annotations.json`.

## Build order

1. **Schema + import** (this scaffold) &mdash; done
2. Keyboard review page (`/review`)
3. Failed + fix page (`/failed`)
4. Dashboard (`/dashboard`)
5. Stack Auth wiring

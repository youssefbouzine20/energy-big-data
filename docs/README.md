# Documentation

This folder is the documentation hub for the project. It complements the
top-level `README.md` (quickstart) and `ethics/GDPR.md` (compliance).

## What belongs here

1. **`data-dictionary.md`** — every field in every Kafka topic / MongoDB collection, with description, type, unit, and example value. Single source of truth for P2/P3/P4/P5.

2. **`architecture.md`** — system architecture diagram, data flow, deployment topology, scaling considerations.

3. **`runbook.md`** — operational playbook: how to start/stop the pipeline, how to recover from common failures (Kafka down, Mongo full, producer crash), how to clear data for a clean demo.

4. **`team-handoff.md`** — per-team contract: what P1 delivers to P2, what P2 delivers to P3, etc. Includes the interface schemas and any agreements made between teammates.

5. **`presentation/`** (subfolder) — defense slides, screenshots, demo recording.

## What does NOT belong here

- Code (lives in `ingestion/`, `processing/`, `storage/`, `ml/`, `dashboard/`)
- Schemas (live in `ingestion/schemas/`)
- GDPR documentation (lives in `ethics/GDPR.md`)
- The README quickstart (lives at project root)

## Suggested structure

```
docs/
├── README.md                # this file
├── data-dictionary.md       # P2/P3/P4/P5 reference
├── architecture.md          # system diagram + flow
├── runbook.md               # operational playbook
├── team-handoff.md          # per-team interface contracts
└── presentation/
    ├── slides.pdf           # defense slides
    ├── screenshots/         # dashboard captures
    └── demo-script.md       # 10-minute demo walkthrough
```

## Priority for the defense

Before the 2026-05-21 defense, the team should have:

- [ ] `data-dictionary.md` — needed by P3/P4/P5 to know what to query
- [ ] `architecture.md` — the slide-1 diagram for the professor
- [ ] `runbook.md` — so the demo doesn't crash on stage
- [ ] `presentation/` — slides and screenshots

## Authoring guidelines

- Use Markdown for everything (renders on GitHub, easy to diff)
- Keep each file under 300 lines — split into multiple if longer
- Link liberally to source files (`[smart_meter.schema.json](../ingestion/schemas/smart_meter.schema.json)`)
- Include concrete examples; avoid abstract descriptions
- Update on every PR that changes the data model or topology

# Documentation Hub

Documentation supporting the 6 deliverables required by the professor's spec.
Complements top-level [README.md](../README.md) (quickstart) and
[ethics/GDPR.md](../ethics/GDPR.md) (compliance).

## The 6 Final Deliverables (from professor's spec — page 2)

The submission ZIP "Projet Final_Big Data_<noms>" sent to i.sassi@uae.ac.ma by
**Thursday 21 May 2026** must contain ALL six:

| # | Deliverable | Location | Owner |
|---|---|---|---|
| 1 | **Code Source** (commented) | `ingestion/`, `processing/`, `storage/`, `ml/`, `dashboard/` | All teams |
| 2 | **Database** (consolidated, structured) | MongoDB dump + schema docs in `storage/` | P3 |
| 3 | **AI Pipeline** (trained, tested, validated) | `ml/models/*.pkl` + comparison table | P5 |
| 4 | **Interactive Dashboard** (deployed, functional) | `dashboard/` running at :8501 | P4 |
| 5 | **Rapport (Methodology Report)** | [`docs/rapport.md`](rapport.md) — TEMPLATE BELOW | All teams contribute |
| 6 | **Présentation** (oral + live demo) | [`docs/presentation/`](presentation/) | All teams |

## Required documents in this folder

| File | Purpose | Priority | Owner |
|---|---|---|---|
| `README.md` | This file — navigation | ✅ exists | P1 |
| `data-dictionary.md` | Every field in every topic + collection (single source of truth) | **HIGH** | P1 + P3 |
| `architecture.md` | System diagram, data flow, deployment topology | **HIGH** | P1 |
| `runbook.md` | How to start/stop/recover (so demo doesn't crash on stage) | **HIGH** | P1 |
| `data-quality-framework.md` | Section E analysis (completeness, noise, bias) | **CRITICAL** | P2 + P5 |
| `tech-justifications.md` | Q&A prep for defense ("Why MongoDB?" etc.) | **CRITICAL** | All teams |
| `rapport.md` | The graded methodology report (deliverable 5) | **CRITICAL** | All teams |
| `presentation/slides.pdf` | 15-min defense slides | **CRITICAL** | All teams |
| `presentation/demo-script.md` | Step-by-step 10-min live demo walkthrough | **CRITICAL** | All teams |

## Rapport template structure (deliverable 5 — graded)

The professor explicitly requires the Rapport to contain:

1. **Méthodologie** — engineering process, sprints, role distribution
2. **Justification des choix technologiques** — graded explicitly ("pourquoi tel algorithme ? pourquoi telle plateforme ? pourquoi telle base NoSQL ?")
   - Why Kafka (vs RabbitMQ, Pulsar)?
   - Why Spark Structured Streaming (vs Flink, Kafka Streams)?
   - Why MongoDB (vs HBase, Cassandra, Neo4j, ElasticSearch)?
   - Why these 3 ML algorithms (vs SVM, neural networks)?
   - Why Streamlit (vs Grafana, Dash)?
3. **Comparaison des modèles ML/MD** — the 6-column table from [`ml/README.md`](../ml/README.md): Precision / Recall / F1 / ROC-AUC / inference time / model size
4. **Étude des performances** — pipeline throughput, end-to-end latency, MongoDB query latency, dashboard load time
5. **Data Quality Framework** — completeness, noise, bias analysis (Section E — see `data-quality-framework.md`)
6. **Recommandations pour les professionnels** — what should a real utility deploying this consider differently? (cost, scale, regulation, multi-tenancy)
7. **Annexes** — schemas, GDPR compliance, deployment modes (local / pseudo / distributed)

## Data Quality Framework (Section E — separate file `data-quality-framework.md`)

Section E of the spec requires "Analyse critique de la qualité des données (complétude, bruit, biais algorithmiques)". The dedicated doc covers:

### Completeness
- Per-field null rate across all topics
- Time coverage: any gaps in 15-min windows over the last 30 days?
- Schema version drift: any messages with `schema_version != "1.0"`?

### Noise
- Schema validation failures rate (jsonschema rejections)
- Out-of-range values (e.g. consumption < 0.001 or > 0.12)
- Producer error rate from `[METRICS]` logs

### Algorithmic biases
- **Class imbalance:** anomaly rate vs target ~7%
- **Zone imbalance:** message count per zone (should be ~equal)
- **Temporal bias:** is training data weighted toward peak hours?
- **Synthetic data limitations:** all data is from simulator — document the gap between synthetic and real-world (e.g. no holiday patterns, no equipment failure modes beyond 4 enums)

## Defense priorities (T-minus 6 days as of 2026-05-15)

Build these in order. Each is required by the professor.

- [ ] Day 1: `data-dictionary.md` — unblocks P2/P3/P4/P5 development
- [ ] Day 2: `architecture.md` — diagrams for slide 1 of the defense
- [ ] Day 3–4: `tech-justifications.md` — answers for every "Why X?" question
- [ ] Day 5: `rapport.md` — assemble all sections from teammates
- [ ] Day 5: `data-quality-framework.md` — Section E coverage
- [ ] Day 6: `runbook.md` — last (after everything works)
- [ ] Day 6: `presentation/slides.pdf` + `demo-script.md` — defense rehearsal

## Authoring guidelines

- Markdown only (renders on GitHub, easy to diff in PRs)
- Each file ≤ 300 lines (split if longer)
- Link to source files: `[smart_meter.schema.json](../ingestion/schemas/smart_meter.schema.json)`
- Concrete examples > abstract descriptions
- Update on every PR that changes data model or topology

## What does NOT belong here

- Code (lives in `ingestion/`, `processing/`, `storage/`, `ml/`, `dashboard/`)
- JSON schemas (live in `ingestion/schemas/`)
- GDPR documentation (lives in `ethics/GDPR.md`)
- Top-level quickstart (lives at project-root `README.md`)
- ML model binaries (`ml/models/*.pkl` — gitignored)

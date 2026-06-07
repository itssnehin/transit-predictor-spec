# CLAUDE.md

Instructions for LLM coding assistants (Claude Code, Cursor, Continue, etc.) operating in this repository.

## First, read these in order

1. `README.md` — orientation
2. `docs/PROJECT_CONTEXT.md` — why and constraints
3. `docs/ARCHITECTURE.md` — system design
4. `docs/DECISIONS.md` — what we already committed to and why
5. `docs/GLOSSARY.md` — terminology
6. The relevant spec in `specs/` for the component you're working on

Only after these are read should you propose code changes.

## Operating Rules

### Scope discipline

This project has documented scope. Out-of-scope items in `PROJECT_CONTEXT.md` are not subject to suggestion. Do not propose:

- Authentication, user accounts, multi-tenancy
- Mobile app, end-user UI beyond a basic dashboard
- Multi-city expansion
- Streaming output beyond the API endpoint
- Use of LLMs in the prediction pipeline (this is a tabular ML project)

If a request seems to drift toward out-of-scope, ask for confirmation before proceeding.

### Architectural fidelity

The architecture in `docs/ARCHITECTURE.md` is committed. Do not propose:

- Replacing XGBoost with a neural network
- Replacing Kafka with a different message broker
- Replacing the local stack with cloud emulators (LocalStack etc.)
- Adopting a different orchestrator
- "Refactoring" the adapter pattern away

If a change to the architecture is genuinely needed, propose it as a new ADR entry in `docs/DECISIONS.md` first. Don't surface it as code.

### Code style

- **Python**: type hints everywhere, `ruff` formatted, `mypy --strict` clean for new code
- **Imports**: stdlib → third-party → local, separated by blank lines
- **Tests**: every new module gets a test file; happy path + at least one edge case
- **Docstrings**: Google style for public functions; one-liner for private
- **No clever code**: clarity over cleverness. The owner needs to be able to explain every line in an interview.

### Dependency rules

- New runtime dependencies require justification in the commit message
- Pin all versions in `pyproject.toml`
- Use `uv` for dependency management, not pip or poetry
- Standard library first, then well-known packages, then niche packages — in that order of preference

### Defaults

- Python 3.11 (not 3.12+; some data libraries lag)
- Spark 3.5.x
- Kafka API (confluent-kafka-python)
- FastAPI 0.110+
- XGBoost 2.x
- dbt-core 1.7+
- pytest 8.x

### Things to flag, not silently fix

When working on a task, surface (don't fix without asking) any of:

- Hardcoded credentials or URLs
- Missing tests for new public functions
- Architectural deviations from the spec
- Spec gaps (something in the spec is unclear or contradictory)
- Cost-relevant cloud resource additions

### What to do when uncertain

Ask. Do not guess. Do not generate plausible-looking code that fills a gap in the spec — the gap itself is the signal that a human decision is needed.

### Reproducibility commitment

For ML code specifically:

- Set seeds for every source of randomness
- Log seeds, code versions, and data hashes to MLflow
- Test that a re-run with same seed produces identical output
- Document any non-determinism that cannot be eliminated

### Cost discipline (when working on infra)

- Never `apply` Terraform changes without explicit approval. Generate plans for review.
- Highlight any resource that costs more than US$5/day at rest.
- Prefer free-tier / pay-per-use over fixed-fee.
- Every cloud resource needs a corresponding `terraform destroy` path.

## Helpful Behaviour

### When implementing a feature

1. Re-read the relevant spec
2. List the files you'll create or modify before touching anything
3. Write tests first if it's testable logic; otherwise write the type signatures first
4. Implement minimally — get the green-path test passing
5. Add edge cases listed in the spec
6. Lint and type-check
7. Summarise what you did in commit message terms

### When the spec is ambiguous

- State the ambiguity precisely
- List the options (with trade-offs)
- Recommend one with reasoning
- Wait for the owner to confirm

### When you find a bug in existing code

- Confirm it's actually a bug (read the relevant test and spec)
- Add a failing test that captures the bug
- Fix the bug
- Verify the fix
- Note it in the commit

## What This File Is Not

- A complete specification (those are in `specs/`)
- A style guide (Ruff config is the source of truth for that)
- Permission to make architectural decisions autonomously

When in doubt, defer to the human.

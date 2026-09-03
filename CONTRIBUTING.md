# Contributing

This is primarily a solo/portfolio project — this document exists to make
the actual engineering workflow explicit and reproducible, not to onboard
external contributors. If you're reading this to evaluate the project rather
than to submit a change, the checks below are exactly what CI enforces on
every push, so `git log` and the CI badge on the README reflect them
directly.

## Setup

```bash
uv sync --group dev     # ruff, ty, sqlfluff, pytest, bandit, pip-audit
cp .env.example .env    # fill in real values — see README.md's
                         # "Configuration reference" for what each does
```

## Before committing

Run the same checks CI runs (`.github/workflows/ci.yml`), in the same order
it does:

```bash
uv run ruff format --check .          # formatting
uv run ruff check .                   # linting
uv run ty check .                     # static type checking
uv run pytest --cov=. --cov-report=term-missing -v   # unit tests (tests/)
uv run sqlfluff lint sql/             # SQL lint
uv run bandit -c pyproject.toml -r .  # static security lint
uv run pip-audit --strict             # dependency vulnerability scan
```

CI additionally lints `openapi.yaml` (Redocly), validates
`docker-compose.yml`/the GitHub Actions YAML (yamllint), and runs a secret
scan (gitleaks) — none of those need network access or credentials to run
locally, but aren't part of the day-to-day inner loop above.

## Commit style

Conventional, imperative-mood subject lines scoped to what changed —
`fix(nifi): ...`, `feat(dbt): ...`, `docs: ...` — matching the existing
history (`git log --oneline`). Keep the body focused on *why*, not a restatement
of the diff.

## Architecture changes

Anything touching the NiFi flow, the bronze schema, or the deployment
topology should also update [`ARCHITECTURE.md`](ARCHITECTURE.md) and/or
[`DEPLOYMENT.md`](DEPLOYMENT.md) in the same change — both are living
documents this project treats as part of the change, not an afterthought
(see either file's own dated, incident-referenced entries for the level of
detail expected).

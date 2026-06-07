# Project Context

## Why This Project Exists

This is a portfolio project for a junior data scientist / data engineer building toward sponsored employment in Australia. It is explicitly designed to demonstrate, in a single coherent codebase, the skills that hiring managers at AWS-shop employers screen for:

- Streaming data ingestion (Kafka)
- Stream processing (Spark Structured Streaming)
- Cloud-native data engineering (S3, Athena, dbt)
- Container orchestration (Kubernetes / EKS)
- Classical machine learning (XGBoost on tabular)
- MLOps (experiment tracking, model registry, automated retraining)
- Infrastructure as code (Terraform)
- CI/CD (GitHub Actions)
- Production observability (metrics, drift monitoring)

The project must end up *deployable to AWS*, but the build path is **local-first**: everything runs on a laptop via Docker Compose before any cloud resource is provisioned.

## What Success Looks Like

### Engineering Success

A reviewer can:

1. `git clone` the repo
2. Run `make up`
3. Observe the system pulling live TransLink data, processing it, and serving predictions at `localhost:8000/predict`
4. Run `make test` and see all tests pass

Within 15 minutes of cloning, with no AWS account required.

### ML Success

The XGBoost model **must beat both baselines** on a held-out test set:

- Naive baseline: predict the last observed delay for that route
- Linear regression on the same features

Target metrics (held-out test set, evaluated on 2-4 weeks of data):

- MAE < 90 seconds for short-horizon (5-min ahead) predictions
- MAE < 180 seconds for 15-min ahead predictions
- Improvement of at least 15% over naive baseline on MAE

These targets are aspirational, not contractual. The *story* of "I tried, here's what worked, here's what didn't" is more important than hitting a specific number.

### Career Success

The project produces:

1. A polished public GitHub repository with comprehensive README, architecture diagrams, and a write-up of findings
2. 3-5 strong resume bullets with quantified outcomes
3. A defensible answer to "tell me about a project you've built" for every variant of that interview question
4. A working live demo that can be screen-shared in interviews
5. Material for a blog post or LinkedIn article

## Constraints

### Financial

- Local-first development phase must cost $0 in cloud bills.
- AWS phase budget: under AU$100/month average, hard cap at AU$200/month.
- All AWS resources must be `terraform destroy`-able. No clicked-up consoles.
- Set AWS Budget alerts at AU$50 and AU$100.
- Free tier maximised; new account if available.

### Time

The owner has a full-time grad role. Realistic capacity is 8-12 hours per week. The project should be scoped for a 6-month build with the local-first MVP shippable by month 3.

### Scope Discipline

This project is one project. It is not a platform. Resist the temptation to add:

- Multi-city support (Brisbane only)
- Multiple transport modes (buses only; no trains, ferries, trams)
- Real-time UI dashboards beyond basic monitoring
- Mobile app
- User accounts / authentication
- Multi-tenancy

If a feature does not directly serve "predict bus delays" or "demonstrate a target skill", cut it.

## Non-Goals

- Production-grade reliability. This is a portfolio project. 99% uptime is fine; 99.9% is over-engineering.
- Beating state-of-the-art on transit delay prediction. The interesting story is the engineering, not the model novelty.
- Real-time UI for end users. Predictions are accessible via API; a basic dashboard is enough.
- Cost optimisation beyond "don't waste money". This is not a FinOps project.

## Stakeholders

The owner is the only stakeholder. There is no client, no manager, no deadline beyond the self-imposed roadmap. Decisions should optimise for the owner's career outcomes and learning.

## Reference Inspirations

Useful patterns from similar projects (do not copy, learn from):

- Citymapper's transit prediction blog posts (architectural patterns)
- Google Maps ETA blog (feature engineering ideas)
- Spotify's MLOps stack write-ups (system design)
- Coiled / dbt Labs reference implementations (modern data stack patterns)

## When To Reach For An LLM

LLM-assisted development is welcome and expected on this project. Good uses:

- Boilerplate (Terraform modules, Kubernetes manifests, FastAPI scaffolding)
- Test generation against existing specs
- Refactoring across files
- SQL/dbt model authoring from a clear schema
- Documentation generation

Bad uses:

- Architectural decisions (the owner makes these; specs document them)
- Model choice or hyperparameter selection (the owner experiments; results are documented)
- Anything that bypasses the learning objectives of the project

The point of this project is for the owner to *understand* every component well enough to defend it in an interview. LLM-generated code that the owner cannot explain is technical debt against the actual goal.

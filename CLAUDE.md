# LakeView — Claude Context

## Project Overview

`LakeView` is a OneHouse component for surfacing lakehouse-table observability — table metadata, partition health, ingestion progress, and similar visibility into Hudi/Iceberg tables managed by OneHouse.

## Documentation

All conceptual / architectural / operational docs live in `docs/`. See [`docs/index.md`](docs/index.md) for the index.

This repo's `docs/` folder is mounted into the OneHouse AI agent platform via git-sync and indexed into QMD as the `lakeview-docs` collection — so well-written docs here are directly consumed by RCA, Scoping, and other agents.

## Coding Principles

- **Document everything.** Every new feature, integration, or significant change MUST have corresponding documentation in `docs/`. Create a new doc file if no existing doc covers the topic. Always keep docs in sync with code — documentation is not optional.

- When opening a PR or pushing a commit that touches behaviour, also update the relevant doc under `docs/`. If your change affects architecture, deployment, or configuration, expand or rewrite the corresponding section. If you add a new module or component, add a doc for it.

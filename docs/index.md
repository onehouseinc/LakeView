# LakeView Documentation

This directory holds conceptual / architectural / operational documentation for the `LakeView` repo. Code-level docstrings live in source files; long-form explanation lives here.

## Index

- [overview.md](overview.md) — what this repo does and where it fits in the OneHouse stack
- [getting-started.md](getting-started.md) — local development setup
- [iceberg-support.md](iceberg-support.md) — Iceberg table discovery, metadata.json selection, and deploy ordering
- [metadata-extractor-flow.md](metadata-extractor-flow.md) — end-to-end code flow of the metadata-extractor data-plane Push-Model job (entry → discovery → metadata read → pre-signed URL fetch → upload → checkpoint), plus retry policy, configuration reference, error matrix, test-coverage map, and debugging recipes

## Adding a doc

Drop a new `<topic>.md` here and link it in this index.

## Indexing automation

This repo's `docs/` folder is mounted into the OneHouse AI agent platform via git-sync and indexed into QMD as the `lakeview-docs` collection. After landing a doc change on `main`, trigger the [reindex workflow](https://github.com/onehouseinc/knowledge-base/actions/workflows/reindex-qmd.yml) or wait up to an hour for the next git-sync tick.

# LakeView — Overview

> **Stub.** Engineers will fill this in.

`LakeView` is a OneHouse component for surfacing lakehouse-table observability — table metadata, partition health, ingestion progress, and similar visibility into Hudi/Iceberg tables managed by OneHouse.

## Components

> List key components / modules — typical entries: storage clients (S3/GCS/Azure), metadata extractors, REST/gRPC API layer.

## Related repos

- `onehouse-dataplane` — produces the table state LakeView surfaces
- `gateway-controller` — control-plane counterpart that LakeView interacts with

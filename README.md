# Stove examples

Showcase applications for [Stove](https://github.com/Trendyol/stove). Each runnable example lives under `jvm/` or `go/` and reports to the dashboard under its domain name.

| Example | Path | Application |
| --- | --- | --- |
| `orders` | `jvm/orders` | Kotlin Spring Boot order service: HTTP, Spring bridge, PostgreSQL, Kafka, WireMock, gRPC client, gRPC mock, db-scheduler, tracing, profiling, dashboard |
| `catalog-process` | `go/catalog-process` | Go catalog service as a host process, with Sarama, franz-go, and segmentio |
| `catalog-container` | `go/catalog-container` | The same catalog service, built from `go/catalog` and run as a container |

`go/catalog` is the shared Go module. The process and container suites compile that module and share `go/catalog/e2e`.

Versions for every Gradle project live in one catalog: [`gradle/libs.versions.toml`](gradle/libs.versions.toml). Shared test and Go build behavior lives in [`build-logic`](build-logic) as `com.trendyol.stove.e2e` and `com.trendyol.stove.go`, which is the base for a later Stove Gradle plugin. `just` reads the Stove version from the catalog and starts `ghcr.io/trendyol/stove-server` at the same snapshot, backed by PostgreSQL.

## Prerequisites

- JDK 21
- Docker, for the Stove server, Testcontainers, and the catalog image
- [just](https://github.com/casey/just)
- Go 1.26 or newer, for the catalog service

## Run

Start the server and every example:

```bash
just run all
```

Start the server and one example:

```bash
just run orders
just run catalog-process
just run catalog-container
```

The dashboard is at [http://127.0.0.1:4040](http://127.0.0.1:4040). Orders and both catalog runs show up as `orders` and `catalog`. PostgreSQL is on `127.0.0.1:5433` (`stove` / `stove`).

```bash
just down    # stop the server, keep the database
just reset   # stop the server and delete the database
just logs
```

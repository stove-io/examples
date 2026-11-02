# Stove examples

Showcase applications for [Stove](https://github.com/Trendyol/stove). Catalog and orders tell one commerce story, with applications under `jvm/`, `go/`, `node/`, and `rust/`.

All applications run the same catalog, checkout, and streaming use cases in [`UseCases.kt`](usecase/e2e/kotlin/com/trendyol/stove/examples/ecomm/e2e/tests/UseCases.kt), implementing the contract in [`USE-CASE.md`](USE-CASE.md). Every app demonstrates scheduled work and gRPC queries; the JVM apps also demonstrate application bean access.

| Example | Path | Application |
| --- | --- | --- |
| `ecomm-ktor` | `jvm/ecomm-ktor` | Kotlin Ktor commerce service, started in-process |
| `ecomm-java` | `jvm/ecomm-java` | Java Spring Boot commerce service, started in-process |
| `ecomm-process` | `go/ecomm-process` | Go commerce service as a host process, using franz-go |
| `ecomm-container` | `go/ecomm-container` | The same Go commerce service, built from `go/ecomm` and run as a container |
| `ecomm-node` | `node/ecomm-node` | TypeScript commerce service running on Node.js as a host process |
| `ecomm-rust` | `rust/ecomm-rust` | Rust commerce service as a host process |

`go/ecomm` is the shared Go module. The process and container suites compile that module. Every suite compiles `usecase/e2e`; the Go, Node.js, and Rust process suites also share their environment mappings in `usecase/process`.

Versions for every Gradle project live in one catalog: [`gradle/libs.versions.toml`](gradle/libs.versions.toml). Shared test and Go build behavior lives in [`build-logic`](build-logic) as `com.trendyol.stove.e2e` and `com.trendyol.stove.go`, which is the base for a later Stove Gradle plugin. `just` reads the Stove version from the catalog and starts `ghcr.io/trendyol/stove-server` at the same snapshot, backed by PostgreSQL.

## Prerequisites

- JDK 21
- Docker, for the Stove server, Testcontainers, and the commerce image
- [just](https://github.com/casey/just)
- Go 1.26 or newer, for the Go commerce service
- Node.js 26 or newer, for `ecomm-node`
- A Rust toolchain, a C compiler, and CMake, for `ecomm-rust` (rdkafka builds its native library)

## Run

Start the server and every example:

```bash
just run all
```

Start the server and one example:

```bash
just run ecomm-ktor
just run ecomm-java
just run ecomm-process
just run ecomm-container
just run ecomm-node
just run ecomm-rust
```

The dashboard is at [http://127.0.0.1:4040](http://127.0.0.1:4040). The Go runs show up as `ecomm-go`. The other suites show up as `ecomm-ktor`, `ecomm-java`, `ecomm-node`, and `ecomm-rust`. PostgreSQL is on `127.0.0.1:5433` (`stove` / `stove`).

## Guided chapters

Run `just demo` for the walkthrough, or run one chapter:

| Command | Evidence |
| --- | --- |
| `just chapter catalog` | REST responses, PostgreSQL migrations and state, traces, and runner configuration |
| `just chapter events` | Kafka publication, consumption and commits, followed by the updated PostgreSQL row |
| `just chapter checkout` | Shared fraud gRPC and inventory/payment HTTP mocks, order persistence, events and customer statistics across every application |
| `just chapter background` | Customer statistics, gRPC queries and confirmation jobs; JVM uses db-scheduler and application bean access, while Go, TypeScript and Rust use River |
| `just chapter streaming` | Persisted orders read incrementally over NDJSON |
| `just chapter diagnostics` | The full checkout with its report, traces and dashboard evidence |

`just demo-failure assertion` deliberately expects the wrong status. `just demo-failure persistence` injects a database failure for the demo user. Both should exit nonzero; normal suites default to `none`. Open the failed run in the dashboard, inspect the failed assertion or exception, follow its trace to the mock request, and compare the database and Kafka snapshots. The dashboard's read-only MCP tools can inspect the same run, traces and snapshots. `just profile ecomm-java`, `just profile ecomm-ktor`, `just profile ecomm-process`, and `just profile ecomm-container` run the JVM or Go profiler; Go coverage is available through the Gradle `e2eTestWithCoverage` tasks when enabled. Select the resulting dashboard run and open Drill Down after the test finishes and its profiles are flushed.

For the events chapter, inspect the `product.created` publication. The `product.update` consumption assertion shows receipt; `product.updated` is emitted after the database write, so the following SQL assertion needs no polling DSL. A separate committed-offset assertion verifies that the malformed record was committed. The chapter also sends an unknown ID and malformed bytes and checks the original header and committed consumption. Those records do not create a successful update.

The Go, TypeScript and Rust suites register [`RiverSystem`](usecase/e2e/kotlin/com/trendyol/stove/examples/ecomm/e2e/setup/RiverSystem.kt) by default. `river { shouldBeExecuted<ConfirmationJob>("order_confirmation") { ... } }` matches the job arguments and waits for River's committed `completed` state; a discarded or cancelled matching job fails with its recorded errors. PostgreSQL notifications wake the assertion, and River evidence appears in Stove reports and snapshots. Each suite installs and removes its own notification trigger.

For diagnostics, start with `just demo-failure persistence` and open the failed `SharedCheckoutTest`. The HTTP step shows the 500 response after a test-only database constraint rejects the order. Inspect the trace, mock requests and database/Kafka snapshots, then use the dashboard's read-only MCP connection to retrieve the same evidence. `just demo-failure assertion` instead shows a successful checkout followed by a deliberately wrong assertion. Runs retain their nonzero status so they can also be used from CI.

Connect an MCP client to `http://localhost:4040/mcp` (substitute `STOVE_HTTP_PORT` if set). Copy the exact run ID from the dashboard and call `stove_diagnose` with `run_id`; use its test ID with `stove_trace` (`view: exceptions`) and `stove_snapshot` (`system: Kafka` or `WireMock`) to inspect the evidence. Dashboard run metadata includes the chapter and demo failure scenario, while test names identify individual scenarios.

The bridge contract tests can be run separately with `cd node/ecomm-node && npm ci && npm test` and `cargo test --locked --manifest-path rust/stove-kafka/Cargo.toml --features rdkafka-adapter`. The Rust build generates its tonic client from the vendored proto using a vendored `protoc`.

For Go coverage, run `./gradlew :go:ecomm-process:e2eTestWithCoverage -Pgo.coverage=true` (or use `:go:ecomm-container:e2eTestWithCoverage`). The HTML report is written under that project's `build/go-coverage/coverage.html`.

| Capability | JVM | Go | TypeScript | Rust |
| --- | --- | --- | --- | --- |
| Kafka publication and consumption interception | yes | yes | yes | yes |
| Application runner | in-process | process/container | process | process |
| Profiling | JVM | Go | — | — |
| Coverage integration | — | Go | — | — |

The Node and Rust observer bridges are local libraries using the pinned contract in [`protocol`](protocol/README.md). They report observer failures without changing Kafka outcomes and do not open a connection when disabled. The Node application is built with `npm run build`.

```bash
just down    # stop the server, keep the database
just reset   # stop the server and delete the database
just logs
```

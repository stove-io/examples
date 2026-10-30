set shell := ["bash", "-euo", "pipefail", "-c"]

stove_version := `sed -n 's/^stove = "\(.*\)"/\1/p' gradle/libs.versions.toml`
http_port := env_var_or_default("STOVE_HTTP_PORT", "4040")

export STOVE_VERSION := stove_version
export STOVE_SERVER_URL := "http://127.0.0.1:" + http_port

default:
    @just --list

# Start PostgreSQL and the snapshot Stove server.
up:
    #!/usr/bin/env bash
    set -euo pipefail
    docker compose up --detach
    port="${STOVE_HTTP_PORT:-4040}"
    for _ in $(seq 1 60); do
      if curl -fsS "http://127.0.0.1:${port}/api/v1/health" >/dev/null 2>&1; then
        echo "Stove: http://127.0.0.1:${port}"
        echo "PostgreSQL: 127.0.0.1:${STOVE_POSTGRES_PORT:-5433} (database/user/password: stove)"
        exit 0
      fi
      sleep 1
    done
    echo "Stove did not become healthy" >&2
    docker compose logs stove
    exit 1

# Stop the server and keep the PostgreSQL volume.
down:
    docker compose down --remove-orphans

# Stop the server and delete the PostgreSQL volume.
reset:
    docker compose down --volumes --remove-orphans

# Follow Stove server logs.
logs:
    docker compose logs --follow stove

# Run one example, or every example. Starts the server first.
run app:
    #!/usr/bin/env bash
    set -euo pipefail
    just up
    if [ "{{app}}" = "all" ]; then
      just --quiet apps | while IFS= read -r name; do
        [ -n "$name" ] || continue
        just run-one "$name"
      done
    else
      just run-one "{{app}}"
    fi

# Example names, one per line.
[private]
apps:
    #!/usr/bin/env bash
    set -euo pipefail
    find jvm go -mindepth 2 -maxdepth 2 -name build.gradle.kts -print \
      | while IFS= read -r file; do
          basename "$(dirname "$file")"
        done \
      | sort

[private]
run-one name:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -f "jvm/{{name}}/build.gradle.kts" ]; then
      project=":jvm:{{name}}:e2eTest"
    elif [ -f "go/{{name}}/build.gradle.kts" ]; then
      project=":go:{{name}}:e2eTest"
    else
      echo "Unknown example '{{name}}'." >&2
      echo "Examples:" >&2
      just --quiet apps | sed 's/^/  /' >&2
      exit 1
    fi
    ./gradlew "$project"

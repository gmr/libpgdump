# Default recipe: run checks
default: check

# Run all checks (format, lint, test)
check: fmt-check lint test

# Build the library
build:
    cargo build

# Run tests
test:
    cargo test

# Run clippy lints
lint:
    cargo clippy -- -D warnings

# Check formatting
fmt-check:
    cargo fmt --check

# Auto-format code
fmt:
    cargo fmt

# Run all checks then build in release mode
release-build: check
    cargo build --release

# Set the release version in Cargo.toml
set-version version:
    #!/usr/bin/env bash
    set -euo pipefail
    current=$(grep '^version' Cargo.toml | head -1 | sed 's/.*"\(.*\)"/\1/')
    if [ "{{version}}" = "$current" ]; then
        echo "Version is already {{version}}"
        exit 1
    fi
    # Use a temp file for portability (BSD sed -i requires arg, GNU doesn't)
    tmp=$(mktemp)
    sed 's/^version = ".*"/version = "{{version}}"/' Cargo.toml > "$tmp"
    mv "$tmp" Cargo.toml
    cargo check
    echo "Updated version: $current -> {{version}}"

# Tag a release (sets version, commits, tags, pushes)
release version: (set-version version)
    git add Cargo.toml Cargo.lock
    git commit -m "Release v{{version}}"
    git tag -a "v{{version}}" -m "v{{version}}"
    git push origin main --tags

# Publish to crates.io (dry run)
publish-dry:
    cargo publish --dry-run

# Publish to crates.io
publish:
    cargo publish

# Clean build artifacts
clean:
    cargo clean

# Bootstrap test environment (start postgres, load schema, generate fixtures)
bootstrap:
    #!/usr/bin/env bash
    set -euo pipefail
    export COMPOSE_DISABLE_ENV_FILE=1
    TEST_HOST=${TEST_HOST:-127.0.0.1}

    docker_exec() {
        if [ "${CI:-}" = 'true' ]; then
            docker exec -t postgres "$@"
        else
            docker compose exec postgres "$@"
        fi
    }

    get_exposed_port() {
        local port
        port=$(docker compose port "$1" --protocol "${3:-tcp}" "$2")
        echo "${port#*:}"
    }

    mkdir -p build/data

    # Set up Python venv for data generation
    if [ "${CI:-}" != 'true' ]; then
        if [ -e ./.venv/bin/activate ]; then
            source ./.venv/bin/activate
        else
            python3 -m venv .venv
            source ./.venv/bin/activate
        fi
    fi
    pip install --quiet faker psycopg

    # Start postgres (local only)
    if [ "${CI:-}" != 'true' ]; then
        printf 'Cleaning environment...'
        docker compose down --remove-orphans --volumes
        echo ' done.'

        printf 'Starting environment...'
        docker compose up --wait --wait-timeout 120
        echo ' done.'
    fi

    if [ "${CI:-}" = 'true' ]; then
        PGPORT=5432
    else
        PGPORT="$(get_exposed_port postgres 5432)"
    fi

    echo "Running pgbench..."
    docker_exec /usr/bin/pgbench -i -U postgres postgres

    printf "Loading fixture schema..."
    docker_exec /usr/bin/psql -U postgres -d postgres -q -o /dev/null -f /fixtures/schema.sql
    echo ' done.'

    printf "Generating fixture data..."
    if [ "${CI:-}" = 'true' ]; then
        PGPASSWORD=postgres python bin/generate-fixture-data.py -U postgres -h localhost -p 5432 -d postgres
    else
        python bin/generate-fixture-data.py -U postgres -h "${TEST_HOST}" -p "${PGPORT}" -d postgres
    fi
    echo ' done.'

    printf "Creating test backups..."
    docker_exec /usr/bin/pg_dump -Fc -U postgres -f /data/dump.not-compressed -d postgres --compress=0
    docker_exec /usr/bin/pg_dump -Fc -U postgres -f /data/dump.compressed -d postgres --compress=9
    docker_exec /usr/bin/pg_dump -Fc -U postgres -f /data/dump.no-data -d postgres --compress=0 -s
    docker_exec /usr/bin/pg_dump -Fc -U postgres -f /data/dump.data-only -d postgres --compress=0 -a
    docker_exec /usr/bin/pg_dump -Fc -U postgres -f /data/dump.inserts -d postgres --compress=0 --inserts
    echo ' done.'

    printf "Fixing permissions..."
    docker_exec chmod -R a+r /data
    echo ' done.'

# Generate test fixture dumps only (requires running postgres)
fixtures:
    #!/usr/bin/env bash
    set -euo pipefail
    export COMPOSE_DISABLE_ENV_FILE=1

    docker_exec() {
        if [ "${CI:-}" = 'true' ]; then
            docker exec -t postgres "$@"
        else
            docker compose exec postgres "$@"
        fi
    }

    mkdir -p build/data

    printf "Creating test backups..."
    docker_exec /usr/bin/pg_dump -Fc -U postgres -f /data/dump.not-compressed -d postgres --compress=0
    docker_exec /usr/bin/pg_dump -Fc -U postgres -f /data/dump.compressed -d postgres --compress=9
    docker_exec /usr/bin/pg_dump -Fc -U postgres -f /data/dump.no-data -d postgres --compress=0 -s
    docker_exec /usr/bin/pg_dump -Fc -U postgres -f /data/dump.data-only -d postgres --compress=0 -a
    docker_exec /usr/bin/pg_dump -Fc -U postgres -f /data/dump.inserts -d postgres --compress=0 --inserts
    echo ' done.'

    printf "Fixing permissions..."
    docker_exec chmod -R a+r /data
    echo ' done.'

#!/bin/bash
# Run acceptance tests for multiple CRDB versions in parallel
# Ports: 24.3→26257/8080, 25.2→26258/8081, 25.4→26259/8082, 26.1→26260/8083

set -u

LOG_DIR="$(pwd)/tests/logs"
mkdir -p "$LOG_DIR"

PIDS=()
VERSION_KEYS=("24.3" "25.2" "25.4" "26.1")
PG_PORTS=("26257" "26258" "26259" "26260")
HTTP_PORTS=("8080" "8081" "8082" "8083")

run_version() {
  local version=$1
  local pg_port=$2
  local http_port=$3
  local log_file="$LOG_DIR/crdb-${version}.log"
  local container_name="crdb-test-${version//\./-}"

  # Clean up any existing container and its volumes
  docker rm -fv "$container_name" > /dev/null 2>&1 || true

  echo "[${version}] Starting container (PG port ${pg_port}, HTTP port ${http_port})..."

  docker run -d \
    --name "$container_name" \
    -p "${pg_port}:26257" \
    -p "${http_port}:8080" \
    -e COCKROACH_USER=crdb \
    -e COCKROACH_PASSWORD=crdb \
    "cockroachdb/cockroach:latest-v${version}" \
    start-single-node --accept-sql-without-tls \
    > /dev/null 2>&1

  # Wait for CRDB to be ready
  local attempt=0
  local max_attempts=30
  while ! curl -sf "http://localhost:${http_port}/health?ready=1" > /dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [ $attempt -ge $max_attempts ]; then
      echo "[${version}] ERROR: Container failed to start after ${max_attempts} attempts" | tee "$log_file"
      docker rm -f "$container_name" > /dev/null 2>&1 || true
      return 1
    fi
    sleep 3
  done

  echo "[${version}] Container ready. Running tests..." | tee "$log_file"

  TF_ACC=true \
  PGHOST=localhost \
  PGPORT="$pg_port" \
  PGUSER=crdb \
  PGPASSWORD=crdb \
  PGSSLMODE=disable \
  PGSUPERUSER=true \
  COCKROACH_USER=crdb \
  COCKROACH_PASSWORD=crdb \
    go test -count=1 ./postgresql -v -timeout 120m 2>&1 | tee -a "$log_file"

  local exit_code=${PIPESTATUS[0]}
  echo "" | tee -a "$log_file"
  echo "[${version}] Tests finished (exit code: $exit_code)" | tee -a "$log_file"

  # Cleanup container and its volumes
  docker rm -fv "$container_name" > /dev/null 2>&1 || true

  return $exit_code
}

echo "Starting parallel test runs: ${VERSION_KEYS[*]}"
echo "Logs: $LOG_DIR"
echo ""

for i in "${!VERSION_KEYS[@]}"; do
  run_version "${VERSION_KEYS[$i]}" "${PG_PORTS[$i]}" "${HTTP_PORTS[$i]}" &
  PIDS+=($!)
done

echo "All versions launched. Waiting for completion..."
echo ""

FAILED=()
for i in "${!VERSION_KEYS[@]}"; do
  version="${VERSION_KEYS[$i]}"
  if wait "${PIDS[$i]}"; then
    echo "[${version}] PASSED"
  else
    echo "[${version}] FAILED"
    FAILED+=("$version")
  fi
done

echo ""
echo "========================================"
echo "SUMMARY"
echo "========================================"

if [ ${#FAILED[@]} -eq 0 ]; then
  echo "All versions PASSED"
else
  echo "Failed versions: ${FAILED[*]}"
  echo ""
  for version in "${FAILED[@]}"; do
    log_file="$LOG_DIR/crdb-${version}.log"
    echo "--- ${version} failures ---"
    grep -E "^--- FAIL|^FAIL" "$log_file" | head -50 || true
    echo ""
  done
fi

echo "========================================"
[ ${#FAILED[@]} -eq 0 ] && exit 0 || exit 1

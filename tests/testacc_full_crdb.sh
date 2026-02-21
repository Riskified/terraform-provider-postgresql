#!/bin/bash
set -e

log() {
  echo "####################"
  echo "## ->  $1 "
  echo "####################"
}

setup() {
    "$(pwd)"/tests/testacc_setup_crdb.sh
}

run() {
  go test -count=1 ./postgresql -v -timeout 120m

  # keep the return value for the scripts to fail and clean properly
  return $?
}

cleanup() {
    "$(pwd)"/tests/testacc_cleanup_crdb.sh
}

log "setup" && setup
source "$(pwd)"/tests/switch_crdb.sh
log "run" && run || (log "cleanup" && cleanup && exit 1)
log "cleanup" && cleanup

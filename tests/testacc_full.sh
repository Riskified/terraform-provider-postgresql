#!/bin/bash
set -e

log() {
  echo "####################"
  echo "## ->  $1 "
  echo "####################"
}

setup() {
    "$(pwd)"/tests/testacc_setup.sh $1
}

run() {
  go test -count=1 ./postgresql -v -timeout 120m
  
  # keep the return value for the scripts to fail and clean properly
  return $?
}

cleanup() {
    "$(pwd)"/tests/testacc_cleanup.sh $1
}

run_suite() {
    suite=${1?}
    tech=${2?}
    log "setup ($1)" && setup "$tech"
    source "./tests/switch_$suite.sh"
    log "run ($1)" && run || (log "cleanup" && cleanup $tech && exit 1)
    log "cleanup ($1)" && cleanup $tech
}


if [ "$1" == "pg" ]; then
    run_suite "superuser" "pg"
    run_suite "rds" "pg"
else
    run_suite "crdb" "crdb"
fi



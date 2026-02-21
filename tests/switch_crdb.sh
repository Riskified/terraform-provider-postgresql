#!/bin/bash

export TF_ACC=true
export PGHOST=localhost
export PGPORT=26257
export PGUSER=${COCKROACH_USER:-crdb}
export PGPASSWORD=${COCKROACH_PASSWORD:-crdb}
export PGSSLMODE=disable
export PGSUPERUSER=true
export COCKROACH_USER=${COCKROACH_USER:-crdb}
export COCKROACH_PASSWORD=${COCKROACH_PASSWORD:-crdb}

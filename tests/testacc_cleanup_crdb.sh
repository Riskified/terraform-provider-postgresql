#!/bin/bash

source "$(pwd)"/tests/switch_crdb.sh
docker compose -f "$(pwd)"/tests/docker-compose-crdb.yml down
unset TF_ACC PGHOST PGPORT PGUSER PGPASSWORD PGSSLMODE PGSUPERUSER

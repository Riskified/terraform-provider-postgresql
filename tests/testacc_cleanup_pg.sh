#!/bin/bash

source "$(pwd)"/tests/switch_superuser_pg.sh
docker compose -f "$(pwd)"/tests/docker-compose-pg.yml down
unset TF_ACC PGHOST PGPORT PGUSER PGPASSWORD PGSSLMODE PGSUPERUSER

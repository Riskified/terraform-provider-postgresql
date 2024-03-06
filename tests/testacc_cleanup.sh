#!/bin/bash

#source "$(pwd)"/tests/switch_superuser.sh
docker compose -f "$(pwd)"/tests/docker-compose-${1}.yml down
unset TF_ACC PGHOST PGPORT PGUSER PGPASSWORD PGSSLMODE PGSUPERUSER

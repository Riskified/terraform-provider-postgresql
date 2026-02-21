#!/bin/bash

source "$(pwd)"/tests/switch_superuser_pg.sh
docker compose -f "$(pwd)"/tests/docker-compose-pg.yml up -d --wait

#!/bin/bash

source "$(pwd)"/tests/switch_crdb.sh
docker compose -f "$(pwd)"/tests/docker-compose-crdb.yml up -d --wait

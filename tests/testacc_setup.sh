#!/bin/bash

source "$(pwd)"/tests/switch_superuser.sh
docker compose -f "$(pwd)"/tests/docker-compose-${1}.yml up -d --wait

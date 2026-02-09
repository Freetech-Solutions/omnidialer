#!/bin/bash

# General settings
export REPO=docker.io/freetechsolutions
export VERSION=250106.01

# Docker images
export API_IMG=${REPO}/dialer_api:${VERSION}
export WORKER_IMG=${REPO}/dialer_worker:${VERSION}
export POSTGRES_IMG=postgres:14.9-bullseye
export GEARMAN_IMG=artefactual/gearmand:1.1.18-alpine
export REDIS_IMG=redis:7.2.5-alpine

export REDIS_DIALER_PORT=6380

export POSTGRES_DIALER_USER=omnidialer
export POSTGRES_DIALER_PASSWORD=dialer456
export POSTGRES_DIALER_SERVER=dialer-postgres
export POSTGRES_DIALER_PORT=5433
export POSTGRES_DIALER_DB=omnidialer

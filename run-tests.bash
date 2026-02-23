#!/usr/bin/env bash

if [ -z "$1" ]
then docker-compose --env-file .env-tests -f docker-compose-test.yml up -d --build --remove-orphans
else docker-compose --env-file .env-tests -f docker-compose-test.yml up -d --remove-orphans
fi

docker exec -it omnidialer-worker-test python -m unittest tests.py

docker-compose --env-file .env-tests -f docker-compose-test.yml down

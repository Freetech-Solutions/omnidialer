#!/usr/bin/env sh
# Docker 24.x (CI dind) soporta hasta API 1.43; 1.44 requiere daemon >= 25.
export DOCKER_API_VERSION=1.43

if [ -z "$1" ]; then
    docker-compose --env-file .env-tests -f docker-compose-test.yml up -d --build --remove-orphans
else
    docker-compose --env-file .env-tests -f docker-compose-test.yml up -d --remove-orphans
fi

docker exec omnidialer-worker-test sh -c "python -m unittest tests.py --failfast; exit \$?"

TEST_EXIT_CODE=$?

docker-compose --env-file .env-tests -f docker-compose-test.yml down

exit $TEST_EXIT_CODE

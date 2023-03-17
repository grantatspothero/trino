#!/usr/bin/env bash

function cleanup {
    if [[ -n ${CONTAINER_ID:-} ]]; then
        docker rm -f "${CONTAINER_ID}"
    fi
}

function test_trino_starts {
    local QUERY_PERIOD=10
    local QUERY_RETRIES=90

    CONTAINER_ID=
    trap cleanup EXIT

    local CONTAINER_NAME=$1
    local PLATFORM=$2
    # We aren't passing --rm here to make sure container is available for inspection in case of failures
    CONTAINER_ID=$(docker run -d --platform "${PLATFORM}" "${CONTAINER_NAME}")

    set +e
    I=0
    until docker inspect "${CONTAINER_ID}" --format "{{json .State.Health.Status }}" | grep -q '"healthy"'; do
        if [[ $((I++)) -ge ${QUERY_RETRIES} ]]; then
            echo "🚨 Too many retries waiting for Trino to start"
            echo "Logs from ${CONTAINER_ID} follow..."
            docker logs "${CONTAINER_ID}"
            break
        fi
        sleep ${QUERY_PERIOD}
    done
    if ! RESULT=$(docker exec "${CONTAINER_ID}" trino --execute "SELECT 'success'" 2>/dev/null); then
        echo "🚨 Failed to execute a query after Trino container started"
    fi
    if ! VERSION_FROM_CONTAINER=$(docker exec "${CONTAINER_ID}" trino --execute "SELECT version()"  --output-format=CSV_UNQUOTED 2>/dev/null); then
        echo "🚨 Failed to execute a query after Trino container started"
    fi
    set -e

    cleanup
    trap - EXIT

    # The version should match.
    DOCKER_TEST_EXPECTED_TRINO_VERSION="${DOCKER_TEST_EXPECTED_TRINO_VERSION:-expected version not set or empty}"
    if ! [[ ${VERSION_FROM_CONTAINER} == "${DOCKER_TEST_EXPECTED_TRINO_VERSION}" ]]; then
        echo "🚨 Version doesn't match: VERSION_FROM_CONTAINER=${VERSION_FROM_CONTAINER}, DOCKER_TEST_EXPECTED_TRINO_VERSION=${DOCKER_TEST_EXPECTED_TRINO_VERSION}." >&2
        return 1
    fi

    if ! [[ ${RESULT} == '"success"' ]]; then
        echo "🚨 Test query didn't return expected result (\"success\")" >&2
        return 1
    fi

    return 0
}

function test_javahome {
    local CONTAINER_NAME=$1
    local PLATFORM=$2
    # Check if JAVA_HOME works
    docker run --rm --platform "${PLATFORM}" "${CONTAINER_NAME}" \
        /bin/bash -c '$JAVA_HOME/bin/java -version' &>/dev/null

    [[ $? == "0" ]]
}

function test_container {
    local CONTAINER_NAME=$1
    local PLATFORM=$2
    echo "🐢 Validating ${CONTAINER_NAME} on platform ${PLATFORM}..."
    test_javahome "${CONTAINER_NAME}" "${PLATFORM}"
    test_trino_starts "${CONTAINER_NAME}" "${PLATFORM}"
    echo "🎉 Validated ${CONTAINER_NAME} on platform ${PLATFORM}"
}

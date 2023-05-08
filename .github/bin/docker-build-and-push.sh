#!/usr/bin/env bash

set -euo pipefail

usage() {
    cat <<EOF 1>&2
Usage: $0 [-h] [-p] [-a <ARCHITECTURES>] [-t <TAG>]
Builds the Galaxy-Trino Docker image

-h       Display help
-a       Build the specified comma-separated architectures, defaults to amd64,arm64
-t       Tag for docker images, defaults to latest
-p       If specified, perform push after docker build
EOF
}

PERFORM_PUSH="--load"
IMAGE_TAG="latest"
ARCHITECTURES=(amd64 arm64)

while getopts ":a:t:ph" o; do
    case "${o}" in
        a)
            IFS=, read -ra ARCHITECTURES <<< "$OPTARG"
            ;;
        t)
            IMAGE_TAG=${OPTARG}
            ;;
        p)
            PERFORM_PUSH="--push"
            ;;
        h)
            usage
            exit 0
            ;;
        *)
            usage
            exit 1
            ;;
    esac
done
shift $((OPTIND - 1))

SOURCE_DIR="../.."

# Retrieve the script directory.
SCRIPT_DIR="${BASH_SOURCE%/*}"
cd ${SCRIPT_DIR}/../../core/docker

# Move to the root directory to run maven for current version.
pushd ${SOURCE_DIR}
TRINO_VERSION=$(./mvnw --quiet help:evaluate -Dexpression=project.version -DforceStdout)
popd

IMAGE_NAME="trino:${IMAGE_TAG}"
echo "Building ${IMAGE_NAME} for Trino ${TRINO_VERSION}"

PREFIX=trino-server-${TRINO_VERSION}
TARBALL=${SOURCE_DIR}/core/trino-server/target/${PREFIX}.tar.gz

WORK_DIR="$(mktemp -d)"
UNPACK_DIR="$(mktemp -d)"

tar -C ${UNPACK_DIR} -xzf ${TARBALL}

mkdir ${WORK_DIR}/${PREFIX}
# `mv` to preserve hardlinks
mv ${UNPACK_DIR}/${PREFIX}/{bin,lib} ${WORK_DIR}/${PREFIX}

PLUGINS="
bigquery
blackhole
elasticsearch
exchange-buffer
exchange-fallbacking-buffer
exchange-filesystem
galaxy-kafka-event-listener
galaxy-query-monitor-event-listener
galaxy-objectstore
galaxy-oracle
galaxy-snowflake
galaxy-synapse
galaxy-warp-speed
geospatial
google-sheets
hive
iceberg
mariadb
memory
mongodb
ml
mysql
postgresql
redshift
resource-group-managers
session-property-managers
sqlserver
teradata-functions
tpcds
tpch
"

mkdir ${WORK_DIR}/${PREFIX}/plugin
for name in $PLUGINS
do
    # `mv` to preserve hardlinks
    mv ${UNPACK_DIR}/${PREFIX}/plugin/${name} ${WORK_DIR}/${PREFIX}/plugin
done

rm -r ${UNPACK_DIR}

cp -R bin ${WORK_DIR}/${PREFIX}
cp -R default ${WORK_DIR}/

cp ${SOURCE_DIR}/client/trino-cli/target/trino-cli-${TRINO_VERSION}-executable.jar ${WORK_DIR}

platforms=("${ARCHITECTURES[@]/#/linux\/}")
docker buildx build --pull ${PERFORM_PUSH} \
   --progress=plain `# Use plain to show container output` \
   --platform "$(IFS=,; echo "${platforms[*]}")" \
   --tag "179619298502.dkr.ecr.us-east-1.amazonaws.com/${IMAGE_NAME}" \
   --tag "us-east1-docker.pkg.dev/starburstdata-saas-prod/starburst-docker-repository/${IMAGE_NAME}" \
   --tag "starburstgalaxy.azurecr.io/${IMAGE_NAME}" \
   --build-arg "TRINO_VERSION=${TRINO_VERSION}" \
   --build-arg "GALAXY_TRINO_DOCKER_VERSION=${IMAGE_TAG}" \
   --file Dockerfile ${WORK_DIR}

rm -r ${WORK_DIR}

echo "🏃 Testing built images"
source container-test.sh
for platform in "${platforms[@]}"; do
    DOCKER_TEST_EXPECTED_TRINO_VERSION="${IMAGE_TAG}" \
        test_container "179619298502.dkr.ecr.us-east-1.amazonaws.com/${IMAGE_NAME}" "${platform}"
done

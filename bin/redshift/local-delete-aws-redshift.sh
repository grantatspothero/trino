#!/usr/bin/env bash

REDSHIFT_SCRIPTS_DIR="${BASH_SOURCE%/*}"
cd "${REDSHIFT_SCRIPTS_DIR}/../.." || exit 1

env AWS_REGION=us-east-2 \
    bin/redshift/delete-aws-redshift.sh

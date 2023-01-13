#!/usr/bin/env bash

# TODO
# TODO Warning: this is just a temporary version of the script, to be replaced.
# TODO It has not been tidied up and doesn't yet correspond to how we write scripts.
# TODO

REDSHIFT_SCRIPTS_DIR="${BASH_SOURCE%/*}"
cd "${REDSHIFT_SCRIPTS_DIR}/../.." || exit 1

env AWS_REGION=us-east-2 \
    bin/redshift/delete-aws-redshift.sh

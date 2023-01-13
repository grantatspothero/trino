#!/usr/bin/env bash

# TODO
# TODO Warning: this is just a temporary version of the script, to be replaced.
# TODO It has not been tidied up and doesn't yet correspond to how we write scripts.
# TODO

REDSHIFT_SCRIPTS_DIR="${BASH_SOURCE%/*}"
cd "${REDSHIFT_SCRIPTS_DIR}/../.." || exit 1

if [[ -f "${REDSHIFT_SCRIPTS_DIR}/.cluster-identifier" ]];  then
    echo "File ${REDSHIFT_SCRIPTS_DIR}/.cluster-identifier already exists. Call ${REDSHIFT_SCRIPTS_DIR}/local-delete-aws-redshift.sh to delete the existing cluster."
    exit 1
fi

env AWS_REGION=us-east-2 \
    REDSHIFT_SUBNET_GROUP_NAME=redshift-cicd-subnet-group \
    REDSHIFT_NODE_TYPE=dc2.large \
    REDSHIFT_DATABASE_NAME=testdb \
    REDSHIFT_IAM_ROLES=arn:aws:iam::888469412714:role/redshift-cicd \
    REDSHIFT_VPC_SECURITY_GROUP_IDS="[\"sg-0ad7ac36a73650b08\", \"sg-012799e2208deb2ab\"]" \
    REDSHIFT_PUBLICLY_ACCESSIBLE=false \
    REDSHIFT_S3_TPCH_TABLES_ROOT="s3://starburstdata-engineering-redshift-test" \
    bin/redshift/setup-aws-redshift.sh

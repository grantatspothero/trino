#!/usr/bin/env bash

REDSHIFT_SCRIPTS_DIR="${BASH_SOURCE%/*}"
cd "${REDSHIFT_SCRIPTS_DIR}/../.." || exit 1

if [[ -f "${REDSHIFT_SCRIPTS_DIR}/.cluster-identifier" ]];  then
    echo "File ${REDSHIFT_SCRIPTS_DIR}/.cluster-identifier already exists. Call ${REDSHIFT_SCRIPTS_DIR}/local_delete_aws_redshift.sh to delete the existing cluster."
    exit 1
fi

export AWS_REGION=us-east-2
export REDSHIFT_SUBNET_GROUP_NAME=redshift-cicd-subnet-group
export REDSHIFT_NODE_TYPE=dc2.large
export REDSHIFT_DATABASE_NAME=testdb
export REDSHIFT_IAM_ROLES=arn:aws:iam::888469412714:role/redshift-cicd
export REDSHIFT_VPC_SECURITY_GROUP_IDS="[\"sg-0ad7ac36a73650b08\", \"sg-012799e2208deb2ab\"]"
export REDSHIFT_S3_TPCH_TABLES_ROOT="s3://starburstdata-engineering-redshift-test"

source ./bin/redshift/setup-aws-redshift.sh

echo "
Steps to run tests interactively:

Checkout the project https://github.com/starburstdata/sep-ci-dev-tools
Run the command `./vpn/vpn` inside this project to establish the tunnel towards the AWS VPC used by the AWS Redshift cluster.


psql command line:
export PGPASSWORD='${REDSHIFT_PASSWORD}'
psql -h ${REDSHIFT_ENDPOINT%:*} -p ${REDSHIFT_PORT} -U ${REDSHIFT_USER} ${REDSHIFT_CLUSTER_DATABASE_NAME}

IntelliJ VM Arguments for Run Configuration:
get those from maven test arguments below prefixed -Dtest.redshift., e.g. -Dtest.redshift.some.argument=\"1234\" and don't forget to remove trailing '\' after each argument

Maven Test:
./mvnw test \\
    -B -Dair.check.skip-all=true -Dmaven.javadoc.skip=true \\
    -Dtest.redshift.jdbc.user=\"${REDSHIFT_USER}\" \\
    -Dtest.redshift.jdbc.password=\"${REDSHIFT_PASSWORD}\" \\
    -Dtest.redshift.jdbc.endpoint=\"${REDSHIFT_ENDPOINT}:${REDSHIFT_PORT}/\" \\
    -Dtest.redshift.s3.tpch.tables.root=\"${REDSHIFT_S3_TPCH_TABLES_ROOT}\" \\
    -Dtest.redshift.iam.role=\"${REDSHIFT_IAM_ROLES}\" \\
    -pl :trino-redshift
"

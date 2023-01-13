#!/usr/bin/env bash

# TODO
# TODO Warning: this is just a temporary version of the script, to be replaced.
# TODO It has not been tidied up and doesn't yet correspond to how we write scripts.
# TODO

REDSHIFT_SCRIPTS_DIR="${BASH_SOURCE%/*}"

lower_case_letters="abcdefghijklmnopqrstuvwxyz"
upper_case_letters="ABCDEFGHIJKLMNOPQRSTUVWXYZ"
# Use a 12 character random sequence and at the end add a random cipher, a random lower case letter and a random upper case letter
REDSHIFT_MASTER_USER_PASSWORD="$(openssl rand -base64 12 | tr -dc 'a-zA-Z0-9')$(($RANDOM % 10))${lower_case_letters:$(( RANDOM % ${#lower_case_letters} )):1}${upper_case_letters:$(( RANDOM % ${#upper_case_letters} )):1}"

REDSHIFT_CLUSTER_IDENTIFIER=trino-redshift-ci-cluster-$(openssl rand -hex 8)
REDSHIFT_CLUSTER_TTL=$(date -u -d "+2 hours" +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || date -u -v "+2H" +"%Y-%m-%dT%H:%M:%SZ")
[[ "$REDSHIFT_PUBLICLY_ACCESSIBLE" = true ]] && REDSHIFT_CLUSTER_ACCESSIBILITY="--publicly-accessible" || REDSHIFT_CLUSTER_ACCESSIBILITY="--no-publicly-accessible"

echo "Creating the Amazon Redshift cluster ${REDSHIFT_CLUSTER_IDENTIFIER} on the region ${AWS_REGION}."
REDSHIFT_CREATE_CLUSTER_OUTPUT=$(aws redshift create-cluster \
  --db-name ${REDSHIFT_DATABASE_NAME} \
  --region ${AWS_REGION} \
  --node-type ${REDSHIFT_NODE_TYPE} \
  --number-of-nodes 1 \
  --master-username admin \
  --master-user-password ${REDSHIFT_MASTER_USER_PASSWORD} \
  --cluster-identifier ${REDSHIFT_CLUSTER_IDENTIFIER} \
  --cluster-subnet-group-name ${REDSHIFT_SUBNET_GROUP_NAME} \
  --cluster-type single-node\
  --vpc-security-group-ids "${REDSHIFT_VPC_SECURITY_GROUP_IDS}" \
  --iam-roles ${REDSHIFT_IAM_ROLES} \
  --automated-snapshot-retention-period 0 \
  ${REDSHIFT_CLUSTER_ACCESSIBILITY} \
  --tags Key=cloud,Value=aws Key=environment,Value=test Key=project,Value=trino-redshift Key=ttl,Value=${REDSHIFT_CLUSTER_TTL})

if [ -z "${REDSHIFT_CREATE_CLUSTER_OUTPUT}" ]; then
    # An exception occurred while trying to create the cluster
    exit 1
fi

echo ${REDSHIFT_CLUSTER_IDENTIFIER} > ${REDSHIFT_SCRIPTS_DIR}/.cluster-identifier
echo "Waiting for the Amazon Redshift cluster ${REDSHIFT_CLUSTER_IDENTIFIER} on the region ${AWS_REGION} to be available."

# Wait for the cluster to become available
aws redshift wait cluster-available \
  --cluster-identifier ${REDSHIFT_CLUSTER_IDENTIFIER}

echo "The Amazon Redshift cluster ${REDSHIFT_CLUSTER_IDENTIFIER} on the region ${AWS_REGION} is available for queries."

set +xu
REDSHIFT_CLUSTER_DESCRIPTION=$(aws redshift describe-clusters --cluster-identifier ${REDSHIFT_CLUSTER_IDENTIFIER})
REDSHIFT_CLUSTER_ENDPOINT=$(echo ${REDSHIFT_CLUSTER_DESCRIPTION} | jq -r '.Clusters[0].Endpoint.Address' )
REDSHIFT_CLUSTER_PORT=$(echo ${REDSHIFT_CLUSTER_DESCRIPTION} | jq -r '.Clusters[0].Endpoint.Port' )
REDSHIFT_CLUSTER_DATABASE_NAME=$(echo ${REDSHIFT_CLUSTER_DESCRIPTION} | jq -r '.Clusters[0].DBName' )
REDSHIFT_CLUSTER_MASTER_USERNAME=$(echo ${REDSHIFT_CLUSTER_DESCRIPTION} | jq -r '.Clusters[0].MasterUsername' )


# If running on GHA, export variables to be used by other scripts
if [[ "${CONTINUOUS_INTEGRATION:-}" = "true" ]]; then
    REDSHIFT_USER="${REDSHIFT_CLUSTER_MASTER_USERNAME}"
    REDSHIFT_PASSWORD="${REDSHIFT_MASTER_USER_PASSWORD}"
    REDSHIFT_ENDPOINT="${REDSHIFT_CLUSTER_ENDPOINT}"
    REDSHIFT_PORT="${REDSHIFT_CLUSTER_PORT}"

    export REDSHIFT_USER
    export REDSHIFT_PASSWORD
    export REDSHIFT_ENDPOINT
    export REDSHIFT_PORT
else
    echo "
Steps to run tests interactively:

psql command line:
export PGPASSWORD='${REDSHIFT_MASTER_USER_PASSWORD}'
psql -h ${REDSHIFT_CLUSTER_ENDPOINT%:*} -p 5439 -U ${REDSHIFT_CLUSTER_MASTER_USERNAME} ${REDSHIFT_CLUSTER_DATABASE_NAME}

IntelliJ VM Arguments for Run Configuration:
get those from maven test arguments below prefixed -Dtest.redshift., e.g. -Dtest.redshift.some.argument=\"1234\" and don't forget to remove trailing '\' after each argument

Maven Test:
./mvnw test \\
    -B -Dair.check.skip-all=true -Dmaven.javadoc.skip=true \\
    -Dtest.redshift.jdbc.user=\"${REDSHIFT_CLUSTER_MASTER_USERNAME}\" \\
    -Dtest.redshift.jdbc.password=\"${REDSHIFT_MASTER_USER_PASSWORD}\" \\
    -Dtest.redshift.jdbc.endpoint=\"${REDSHIFT_CLUSTER_ENDPOINT}:${REDSHIFT_CLUSTER_PORT}/\" \\
    -Dtest.redshift.s3.tpch.tables.root=\"${REDSHIFT_S3_TPCH_TABLES_ROOT}\" \\
    -Dtest.redshift.iam.role=\"${REDSHIFT_IAM_ROLES}\" \\
    -pl :trino-redshift
" > "/tmp/.redshift-${REDSHIFT_CLUSTER_IDENTIFIER}"
    echo "Check out /tmp/.redshift-${REDSHIFT_CLUSTER_IDENTIFIER} for connection details to the Amazon Redshift cluster ${REDSHIFT_CLUSTER_IDENTIFIER}"
fi

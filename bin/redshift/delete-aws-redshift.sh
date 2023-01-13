#!/usr/bin/env bash

# TODO
# TODO Warning: this is just a temporary version of the script, to be replaced.
# TODO It has not been tidied up and doesn't yet correspond to how we write scripts.
# TODO

REDSHIFT_SCRIPTS_DIR="${BASH_SOURCE%/*}"

if [[ ! -f "${REDSHIFT_SCRIPTS_DIR}/.cluster-identifier" ]];  then
    echo "Missing file ${REDSHIFT_SCRIPTS_DIR}/.cluster-identifier"
    exit 0
fi

REDSHIFT_CLUSTER_IDENTIFIER=$(cat $REDSHIFT_SCRIPTS_DIR/.cluster-identifier)

echo "Deleting Amazon Redshift cluster $REDSHIFT_CLUSTER_IDENTIFIER"
REDSHIFT_DELETE_CLUSTER_OUTPUT=$(aws redshift delete-cluster --cluster-identifier $REDSHIFT_CLUSTER_IDENTIFIER --skip-final-cluster-snapshot)

if [ -z "${REDSHIFT_DELETE_CLUSTER_OUTPUT}" ]; then
    # An exception occurred while trying to delete the cluster
    exit 1
fi

echo "Waiting for the Amazon Redshift cluster $REDSHIFT_CLUSTER_IDENTIFIER to be deleted"
aws redshift wait cluster-deleted \
  --cluster-identifier $REDSHIFT_CLUSTER_IDENTIFIER
echo "Amazon Redshift cluster $REDSHIFT_CLUSTER_IDENTIFIER has been deleted"

rm -f ${REDSHIFT_SCRIPTS_DIR}/.cluster-identifier
exit 0

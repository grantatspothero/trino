# Redshift Connector

To run the Redshift tests you will need to provision a Redshift cluster.  The
tests are designed to run on the smallest possible Redshift cluster containing
is a single dc2.large instance. Additionally, you will need a S3 bucket 
containing TPCH tiny data in Parquet format.  The files should be named:

```
s3://<your_bucket>/tpch/tiny/<table_name>.parquet
```

To run the tests set the following system properties:

```
test.redshift.jdbc.endpoint=<your_endpoint>.<your_region>.redshift.amazonaws.com:5439/
test.redshift.jdbc.user=<username>
test.redshift.jdbc.password=<password>
test.redshift.s3.tpch.tables.root=<your_bucket>
test.redshift.iam.role=<your_iam_arm_to_access_bucket>
```

## Redshift Ephemeral Cluster

The CI tests spin up a Redshift cluster on-demand for test execution. The Maven tests run first which populate the users and test data
needed for the product tests to run. The scripts under `stargate-trino/bin/redshift` control creating and destroying a cluster which
the `ci.yml` file uses. They can also be used for local development.

### Local Development with Redshift Ephemeral Cluster

1. Make sure you are authenticated to the **starburstdata-engineering (888469412714)** AWS account. e.g. `gimme-aws-creds --role /888469412714/`.
2. Execute `./bin/redshift/local-setup-aws-redshift.sh`. This will spin up a Redshift cluster via bash scripts.
    * It takes roughly four minutes for the script to complete
3. Once finished, you can use the instructions printed in the output of the script previously mentioned to connect 
   directly to AWS Redshift via `psql` or use VM arguments in IntelliJ for the integration tests.
4. Install the `vpn` tool so that the ephemeral Redshift cluster can be connected locally. See instructions at https://github.com/starburstdata/sep-ci-dev-tools/tree/main/vpn.
5. **Important!** Once you are done, run `./bin/redshift/local-delete-aws-redshift.sh` to delete the cluster.

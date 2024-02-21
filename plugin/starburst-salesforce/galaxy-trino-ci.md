## CI Tests 

- Tests do not run on every Pull request
- Test runs only for the pull requests' having `salesforce` label
- Tests are scheduled to run every night

### Test resource
Salesforce test instance is shared across `cork`, `galaxy-trino`, `stargate` repositories.

**Note:** The tests use basic user/password auth. User's password periodically expires every three months.
Tests using expired password will fail with an `INVALID_OPERATION_WITH_EXPIRED_PASSWORD` error.
A new password will need to be created upon expiration, which generates a new security token as well.

1. Login to https://starburstdata--partial.sandbox.my.salesforce.com/ as `sep.salesforcedl.test2@starburstdata.com` and the current (expired) password
2. You will be prompted to create a new password. Once created, an email is sent to `sep.salesforcedl.test2@starburstdata.com` containing the new security token
   * Following associates are part of the DL.  
     Wojciech Biela, Piotr Findeisen, Mateusz Gajewski, Ashhar Hasan, Grzegorz Kokosinski, Anu Sudarsan, Mayank Vadariya
3. Update the GitHub secret value for `SALESFORCE_PASSWORD` and `SALESFORCE_SECRET_TOKEN` to the new values
4. Let the CI build run; the tests should no longer fail with an expired password error
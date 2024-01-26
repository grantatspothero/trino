/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.salesforce;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import com.starburstdata.presto.plugin.salesforce.testing.datatype.SalesforceCreateAndInsertDataSetup;
import com.starburstdata.presto.plugin.salesforce.testing.datatype.SqlDataTypeTest;
import com.starburstdata.presto.plugin.salesforce.testing.sql.SalesforceTestTable;
import io.trino.Session;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.VarcharType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingSession;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.sql.JdbcSqlExecutor;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.starburstdata.presto.plugin.salesforce.SalesforceConfig.SalesforceAuthenticationType.OAUTH_JWT;
import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.ALLOW_OVERFLOW;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.DECIMAL_DEFAULT_SCALE;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.DECIMAL_MAPPING;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.DECIMAL_ROUNDING_MODE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static java.lang.String.format;
import static java.math.RoundingMode.DOWN;
import static java.math.RoundingMode.HALF_UP;
import static java.math.RoundingMode.UNNECESSARY;
import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ISO_DATE;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// Note that there are no CTAS-based type mapping tests in this class due to Salesforce custom object limits
// We instead use static test names with no random suffix to avoid creating and deleting many tables and hitting this limit, which caused builds to fail
// These tables are created if they don't exist and truncated when the test is closed as well as initially to ensure there is no data
// Custom objects can be deleted by hand in Salesforce using the Object Manager
@Test(singleThreaded = true) // Decimal tests share the same table
public class TestSalesforceTypeMapping
        extends AbstractTestQueryFramework
{
    private final ZoneId jvmZone = ZoneId.systemDefault();

    // no DST in 1970, but has DST in later years (e.g. 2018)
    private final ZoneId vilnius = ZoneId.of("Europe/Vilnius");

    // minutes offset change since 1970-01-01, no DST
    private final ZoneId kathmandu = ZoneId.of("Asia/Kathmandu");

    private String jdbcUrl;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        SalesforceOAuthJwtConfig oAuthConfig = new SalesforceOAuthJwtConfig()
                .setPkcs12CertificateSubject("*")
                .setPkcs12Path(Resources.getResource("salesforce-ca.p12").getPath())
                .setPkcs12Password(requireNonNull(System.getProperty("salesforce.test.user1.pkcs12.password"), "salesforce.test.user1.pkcs12.password is not set"))
                .setJwtIssuer("3MVG9OI03ecbG2Vr3NBmmhtNrcBp3Ywy2y0XHbRN_uGz_zYWqKozppyAOX27EWcrOH5HAib9Cd2i8E8g.rYD.")
                .setJwtSubject(requireNonNull(System.getProperty("salesforce.test.user1.jwt.subject"), "salesforce.test.user1.jwt.subject is not set"));

        SalesforceConfig config = new SalesforceConfig()
                .setAuthenticationType(OAUTH_JWT)
                .setSandboxEnabled(true);

        // Create a SalesforceConfig to get the JDBC URL that we can forward to the testing classes
        // to reset the metadata cache between runs
        // Note that if the JDBC connection properties are different than what is used by the
        // QueryRunner than we may have issues with the metadata cache
        this.jdbcUrl = new SalesforceModule.OAuthJwtConnectionUrlProvider(config, oAuthConfig).get();
        return SalesforceQueryRunner.builder()
                .enableWrites() // Enable writes so we can create tables and write data to them
                .setTables(ImmutableList.of()) // No tables needed for type mapping tests
                .build();
    }

    @Test
    public void testDouble()
    {
        // Max numeric value is 18 precision 0 scale when creating double type
        SqlDataTypeTest.create()
                .addRoundTrip("double", "-1.0E17", DoubleType.DOUBLE, "-1.0E17")
                .addRoundTrip("double", "1.0E17", DoubleType.DOUBLE, "1.0E17")
                .addRoundTrip("double", "NULL", DoubleType.DOUBLE, "CAST(NULL as DOUBLE)")
                .execute(getQueryRunner(), salesforceCreateAndInsert("test_double"));
    }

    @Test
    public void testDecimal()
    {
        // Note: The decimal test cases only work because the table's data types were changed in Salesforce after the initial run
        // The 'Currency' data type in Salesforce is the only thing the driver returns as a decimal, and there is no way to
        // create a table with a 'Currency' data type using the JDBC driver

        // If the table gets deleted, this test case will fail until someone goes into the Salesforce Object Manager and manually
        // edits each column to change the data type to a Currency type
        // Object Manager -> test_decimal -> Fields & Relationships -> for each col_*:
        //   Edit -> Change Field Type -> Currency -> Next -> Save
        // Note the max precision for Salesforce is 18
        SqlDataTypeTest.create()
                .addRoundTrip("decimal(3, 0)", "CAST('193' AS decimal(3, 0))", createDecimalType(3, 0), "CAST('193' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 0)", "CAST('19' AS decimal(3, 0))", createDecimalType(3, 0), "CAST('19' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 0)", "CAST('-193' AS decimal(3, 0))", createDecimalType(3, 0), "CAST('-193' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 1)", "CAST('10.0' AS decimal(3, 1))", createDecimalType(3, 1), "CAST('10.0' AS decimal(3, 1))")
                .addRoundTrip("decimal(3, 1)", "CAST('10.1' AS decimal(3, 1))", createDecimalType(3, 1), "CAST('10.1' AS decimal(3, 1))")
                .addRoundTrip("decimal(3, 1)", "CAST('-10.1' AS decimal(3, 1))", createDecimalType(3, 1), "CAST('-10.1' AS decimal(3, 1))")
                .addRoundTrip("decimal(4, 2)", "CAST('2' AS decimal(4, 2))", createDecimalType(4, 2), "CAST('2' AS decimal(4, 2))")
                .addRoundTrip("decimal(4, 2)", "CAST('2.3' AS decimal(4, 2))", createDecimalType(4, 2), "CAST('2.3' AS decimal(4, 2))")
                .addRoundTrip("decimal(16, 2)", "CAST('2' AS decimal(16, 2))", createDecimalType(16, 2), "CAST('2' AS decimal(16, 2))")
                .addRoundTrip("decimal(16, 2)", "CAST('2.3' AS decimal(16, 2))", createDecimalType(16, 2), "CAST('2.3' AS decimal(16, 2))")
                .addRoundTrip("decimal(16, 2)", "CAST('123456789.3' AS decimal(16, 2))", createDecimalType(16, 2), "CAST('123456789.3' AS decimal(16, 2))")
                .addRoundTrip("decimal(14, 4)", "CAST('1234567890.31' AS decimal(14, 4))", createDecimalType(14, 4), "CAST('1234567890.31' AS decimal(14, 4))")
                .addRoundTrip("decimal(13, 5)", "CAST('31415926.38327' AS decimal(13, 5))", createDecimalType(13, 5), "CAST('31415926.38327' AS decimal(13, 5))")
                .addRoundTrip("decimal(13, 5)", "CAST('-31415926.38327' AS decimal(13, 5))", createDecimalType(13, 5), "CAST('-31415926.38327' AS decimal(13, 5))")
                // Note that some large values like '271828182845904523' don't get stored correctly
                // Salesforce UI says 271828182845904500 but the JDBC driver says 271828182845904512
                // Choosing 271828182845904512 because the test case passes. Feels great.
                .addRoundTrip("decimal(18, 0)", "CAST('271828182845904512' AS decimal(18, 0))", createDecimalType(18, 0), "CAST('271828182845904512' AS decimal(18, 0))")
                .addRoundTrip("decimal(18, 0)", "CAST('-271828182845904512' AS decimal(18, 0))", createDecimalType(18, 0), "CAST('-271828182845904512' AS decimal(18, 0))")
                .addRoundTrip("decimal(3, 0)", "NULL", createDecimalType(3, 0), "CAST(NULL AS decimal(3, 0))")
                .addRoundTrip("decimal(18, 0)", "NULL", createDecimalType(18, 0), "CAST(NULL AS decimal(18, 0))")
                .execute(getQueryRunner(), salesforceCreateAndInsert("test_decimal"));
    }

    @Test
    public void testDecimalWithRoundingModeHalfUp()
    {
        DecimalType expectedType = createDecimalType(38, 0);
        SqlDataTypeTest.create()
                .addRoundTrip("decimal(3, 0)", "CAST('193' AS decimal(3, 0))", expectedType, "CAST('193' AS decimal(38, 0))")
                .addRoundTrip("decimal(3, 0)", "CAST('19' AS decimal(3, 0))", expectedType, "CAST('19' AS decimal(38, 0))")
                .addRoundTrip("decimal(3, 0)", "CAST('-193' AS decimal(3, 0))", expectedType, "CAST('-193' AS decimal(38, 0))")
                .addRoundTrip("decimal(3, 1)", "CAST('10.0' AS decimal(3, 1))", expectedType, "CAST('10' AS decimal(38, 0))")
                .addRoundTrip("decimal(3, 1)", "CAST('10.1' AS decimal(3, 1))", expectedType, "CAST('10' AS decimal(38, 0))")
                .addRoundTrip("decimal(3, 1)", "CAST('-10.1' AS decimal(3, 1))", expectedType, "CAST('-10' AS decimal(38, 0))")
                .addRoundTrip("decimal(4, 2)", "CAST('2' AS decimal(4, 2))", expectedType, "CAST('2' AS decimal(38, 0))")
                .addRoundTrip("decimal(4, 2)", "CAST('2.7' AS decimal(4, 2))", expectedType, "CAST('3' AS decimal(38, 0))")
                .addRoundTrip("decimal(16, 2)", "CAST('2' AS decimal(16, 2))", expectedType, "CAST('2' AS decimal(38, 0))")
                .addRoundTrip("decimal(16, 2)", "CAST('2.7' AS decimal(16, 2))", expectedType, "CAST('3' AS decimal(38, 0))")
                .addRoundTrip("decimal(16, 2)", "CAST('123456789.3' AS decimal(16, 2))", expectedType, "CAST('123456789' AS decimal(38, 0))")
                .addRoundTrip("decimal(14, 4)", "CAST('1234567890.31' AS decimal(14, 4))", expectedType, "CAST('1234567890' AS decimal(38, 0))")
                .addRoundTrip("decimal(13, 5)", "CAST('31415926.38327' AS decimal(13, 5))", expectedType, "CAST('31415926' AS decimal(38, 0))")
                .addRoundTrip("decimal(13, 5)", "CAST('-31415926.58527' AS decimal(13, 5))", expectedType, "CAST('-31415927' AS decimal(38, 0))")
                .addRoundTrip("decimal(18, 0)", "CAST('271828182845904512' AS decimal(18, 0))", expectedType, "CAST('271828182845904512' AS decimal(38, 0))")
                .addRoundTrip("decimal(18, 0)", "CAST('-271828182845904512' AS decimal(18, 0))", expectedType, "CAST('-271828182845904512' AS decimal(38, 0))")
                .addRoundTrip("decimal(3, 0)", "NULL", expectedType, "CAST(NULL AS decimal(38, 0))")
                .addRoundTrip("decimal(18, 0)", "NULL", expectedType, "CAST(NULL AS decimal(38, 0))")
                .execute(getQueryRunner(), sessionWithDecimalMappingAllowOverflow(HALF_UP, 0), salesforceCreateAndInsert("test_decimal"));
    }

    @Test
    public void testDecimalWithRoundingModeHalfUpScale2()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("decimal(3, 0)", "CAST('193' AS decimal(3, 0))", createDecimalType(38, 0), "CAST('193' AS decimal(38, 0))")
                .addRoundTrip("decimal(3, 0)", "CAST('19' AS decimal(3, 0))", createDecimalType(38, 0), "CAST('19' AS decimal(38, 0))")
                .addRoundTrip("decimal(3, 0)", "CAST('-193' AS decimal(3, 0))", createDecimalType(38, 0), "CAST('-193' AS decimal(38, 0))")
                .addRoundTrip("decimal(3, 1)", "CAST('10.0' AS decimal(3, 1))", createDecimalType(38, 1), "CAST('10.0' AS decimal(38, 1))")
                .addRoundTrip("decimal(3, 1)", "CAST('10.1' AS decimal(3, 1))", createDecimalType(38, 1), "CAST('10.1' AS decimal(38, 1))")
                .addRoundTrip("decimal(3, 1)", "CAST('-10.1' AS decimal(3, 1))", createDecimalType(38, 1), "CAST('-10.1' AS decimal(38, 1))")
                .addRoundTrip("decimal(4, 2)", "CAST('2' AS decimal(4, 2))", createDecimalType(38, 2), "CAST('2' AS decimal(38, 2))")
                .addRoundTrip("decimal(4, 2)", "CAST('2.7' AS decimal(4, 2))", createDecimalType(38, 2), "CAST('2.7' AS decimal(38, 2))")
                .addRoundTrip("decimal(16, 2)", "CAST('2' AS decimal(16, 2))", createDecimalType(38, 2), "CAST('2' AS decimal(38, 2))")
                .addRoundTrip("decimal(16, 2)", "CAST('2.7' AS decimal(16, 2))", createDecimalType(38, 2), "CAST('2.7' AS decimal(38, 2))")
                .addRoundTrip("decimal(16, 2)", "CAST('123456789.3' AS decimal(16, 2))", createDecimalType(38, 2), "CAST('123456789.3' AS decimal(38, 2))")
                .addRoundTrip("decimal(14, 4)", "CAST('1234567890.31' AS decimal(14, 4))", createDecimalType(38, 2), "CAST('1234567890.31' AS decimal(38, 2))")
                .addRoundTrip("decimal(13, 5)", "CAST('31415926.58327' AS decimal(13, 5))", createDecimalType(38, 2), "CAST('31415926.58' AS decimal(38, 2))")
                .addRoundTrip("decimal(13, 5)", "CAST('-31415926.58527' AS decimal(13, 5))", createDecimalType(38, 2), "CAST('-31415926.59' AS decimal(38, 2))")
                .addRoundTrip("decimal(18, 0)", "CAST('271828182845904512' AS decimal(18, 0))", createDecimalType(38, 0), "CAST('271828182845904512' AS decimal(38, 0))")
                .addRoundTrip("decimal(18, 0)", "CAST('-271828182845904512' AS decimal(18, 0))", createDecimalType(38, 0), "CAST('-271828182845904512' AS decimal(38, 0))")
                .addRoundTrip("decimal(3, 0)", "NULL", createDecimalType(38, 0), "CAST(NULL AS decimal(38, 0))")
                .addRoundTrip("decimal(18, 0)", "NULL", createDecimalType(38, 0), "CAST(NULL AS decimal(38, 0))")
                .execute(getQueryRunner(), sessionWithDecimalMappingAllowOverflow(HALF_UP, 2), salesforceCreateAndInsert("test_decimal"));
    }

    @Test
    public void testDecimalWithRoundingModeRoundDown()
    {
        DecimalType expectedType = createDecimalType(38, 0);
        SqlDataTypeTest.create()
                .addRoundTrip("decimal(3, 0)", "CAST('193' AS decimal(3, 0))", expectedType, "CAST('193' AS decimal(38, 0))")
                .addRoundTrip("decimal(3, 0)", "CAST('19' AS decimal(3, 0))", expectedType, "CAST('19' AS decimal(38, 0))")
                .addRoundTrip("decimal(3, 0)", "CAST('-193' AS decimal(3, 0))", expectedType, "CAST('-193' AS decimal(38, 0))")
                .addRoundTrip("decimal(3, 1)", "CAST('10.0' AS decimal(3, 1))", expectedType, "CAST('10' AS decimal(38, 0))")
                .addRoundTrip("decimal(3, 1)", "CAST('10.1' AS decimal(3, 1))", expectedType, "CAST('10' AS decimal(38, 0))")
                .addRoundTrip("decimal(3, 1)", "CAST('-10.1' AS decimal(3, 1))", expectedType, "CAST('-10' AS decimal(38, 0))")
                .addRoundTrip("decimal(4, 2)", "CAST('2' AS decimal(4, 2))", expectedType, "CAST('2' AS decimal(38, 0))")
                .addRoundTrip("decimal(4, 2)", "CAST('2.7' AS decimal(4, 2))", expectedType, "CAST('2' AS decimal(38, 0))")
                .addRoundTrip("decimal(16, 2)", "CAST('2' AS decimal(16, 2))", expectedType, "CAST('2' AS decimal(38, 0))")
                .addRoundTrip("decimal(16, 2)", "CAST('2.7' AS decimal(16, 2))", expectedType, "CAST('2' AS decimal(38, 0))")
                .addRoundTrip("decimal(16, 2)", "CAST('123456789.3' AS decimal(16, 2))", expectedType, "CAST('123456789' AS decimal(38, 0))")
                .addRoundTrip("decimal(14, 4)", "CAST('1234567890.31' AS decimal(14, 4))", expectedType, "CAST('1234567890' AS decimal(38, 0))")
                .addRoundTrip("decimal(13, 5)", "CAST('31415926.58327' AS decimal(13, 5))", expectedType, "CAST('31415926' AS decimal(38, 0))")
                .addRoundTrip("decimal(13, 5)", "CAST('-31415926.58327' AS decimal(13, 5))", expectedType, "CAST('-31415926' AS decimal(38, 0))")
                .addRoundTrip("decimal(18, 0)", "CAST('271828182845904512' AS decimal(18, 0))", expectedType, "CAST('271828182845904512' AS decimal(38, 0))")
                .addRoundTrip("decimal(18, 0)", "CAST('-271828182845904512' AS decimal(18, 0))", expectedType, "CAST('-271828182845904512' AS decimal(38, 0))")
                .addRoundTrip("decimal(3, 0)", "NULL", expectedType, "CAST(NULL AS decimal(38, 0))")
                .addRoundTrip("decimal(18, 0)", "NULL", expectedType, "CAST(NULL AS decimal(38, 0))")
                .execute(getQueryRunner(), sessionWithDecimalMappingAllowOverflow(DOWN, 0), salesforceCreateAndInsert("test_decimal"));
    }

    @Test
    public void testDecimalWithRoundingModeUnnecessary()
    {
        // We use the test table here to ensure the test_decimal table is empty before and after the test case runs
        try (SalesforceTestTable ignored = new SalesforceTestTable(jdbcUrl, "test_decimal", "")) {
            assertUpdate("INSERT INTO test_decimal__c (col_7__c) VALUES (CAST('2.7' AS decimal(4, 2)))", 1);

            assertQueryFails(sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 0),
                    "SELECT col_7__c FROM test_decimal__c",
                    "Rounding necessary");
        }
    }

    private Session sessionWithDecimalMappingAllowOverflow(RoundingMode roundingMode, int scale)
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty("salesforce", DECIMAL_MAPPING, ALLOW_OVERFLOW.name())
                .setCatalogSessionProperty("salesforce", DECIMAL_ROUNDING_MODE, roundingMode.name())
                .setCatalogSessionProperty("salesforce", DECIMAL_DEFAULT_SCALE, Integer.toString(scale))
                .build();
    }

    @Test
    public void testSalesforceCreatedParameterizedVarchar()
    {
        // Maximum varchar length is 255 and you must specify a length
        SqlDataTypeTest.create()
                .addRoundTrip("VARCHAR(10)", "'text_a'", VarcharType.createVarcharType(10), "CAST('text_a' AS VARCHAR(10))")
                .addRoundTrip("VARCHAR(255)", "'text_b'", VarcharType.createVarcharType(255), "CAST('text_b' AS VARCHAR(255))")
                .execute(getQueryRunner(), salesforceCreateAndInsert("test_param_varchar"));
    }

    @Test
    public void testDate()
    {
        ZoneId jvmZone = ZoneId.systemDefault();
        checkState(jvmZone.getId().equals("America/Bahia_Banderas"), "This test assumes certain JVM time zone");
        LocalDate dateOfLocalTimeChangeForwardAtMidnightInJvmZone = LocalDate.of(1970, 1, 1);
        checkIsGap(jvmZone, dateOfLocalTimeChangeForwardAtMidnightInJvmZone.atStartOfDay());

        ZoneId someZone = ZoneId.of("Europe/Vilnius");
        LocalDate dateOfLocalTimeChangeForwardAtMidnightInSomeZone = LocalDate.of(1983, 4, 1);
        checkIsGap(someZone, dateOfLocalTimeChangeForwardAtMidnightInSomeZone.atStartOfDay());
        LocalDate dateOfLocalTimeChangeBackwardAtMidnightInSomeZone = LocalDate.of(1983, 10, 1);
        checkIsDoubled(someZone, dateOfLocalTimeChangeBackwardAtMidnightInSomeZone.atStartOfDay().minusMinutes(1));

        String dateOfLocalTimeChangeForwardAtMidnightInJvmZoneLiteral = format("DATE '%s'", ISO_DATE.format(dateOfLocalTimeChangeForwardAtMidnightInJvmZone));
        String dateOfLocalTimeChangeForwardAtMidnightInSomeZoneLiteral = format("DATE '%s'", ISO_DATE.format(dateOfLocalTimeChangeForwardAtMidnightInSomeZone));
        String dateOfLocalTimeChangeBackwardAtMidnightInSomeZoneLiteral = format("DATE '%s'", ISO_DATE.format(dateOfLocalTimeChangeBackwardAtMidnightInSomeZone));

        SqlDataTypeTest testCases = SqlDataTypeTest.create()
                .addRoundTrip("DATE", "DATE '1952-04-03'", DateType.DATE, "DATE '1952-04-03'") // before epoch
                .addRoundTrip("DATE", "DATE '1970-01-01'", DateType.DATE, "DATE '1970-01-01'")
                .addRoundTrip("DATE", "DATE '1970-02-03'", DateType.DATE, "DATE '1970-02-03'")
                .addRoundTrip("DATE", "DATE '2017-07-01'", DateType.DATE, "DATE '2017-07-01'") // summer on northern hemisphere (possible DST)
                .addRoundTrip("DATE", "DATE '2017-01-01'", DateType.DATE, "DATE '2017-01-01'") // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip("DATE", dateOfLocalTimeChangeForwardAtMidnightInJvmZoneLiteral, DateType.DATE, dateOfLocalTimeChangeForwardAtMidnightInJvmZoneLiteral)
                .addRoundTrip("DATE", dateOfLocalTimeChangeForwardAtMidnightInSomeZoneLiteral, DateType.DATE, dateOfLocalTimeChangeForwardAtMidnightInSomeZoneLiteral)
                .addRoundTrip("DATE", dateOfLocalTimeChangeBackwardAtMidnightInSomeZoneLiteral, DateType.DATE, dateOfLocalTimeChangeBackwardAtMidnightInSomeZoneLiteral);

        for (String timeZoneId : ImmutableList.of(UTC_KEY.getId(), jvmZone.getId(), someZone.getId())) {
            Session session = Session.builder(getQueryRunner().getDefaultSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(timeZoneId))
                    .build();
            testCases.execute(getQueryRunner(), session, salesforceCreateAndInsert("test_date"));
        }

        // The min and max value in Salesforce
        SqlDataTypeTest.create()
                .addRoundTrip("DATE", "DATE '1700-01-01'", DateType.DATE, "DATE '1700-01-01'")
                .addRoundTrip("DATE", "DATE '4000-12-31'", DateType.DATE, "DATE '4000-12-31'")
                .execute(getQueryRunner(), salesforceCreateAndInsert("test_date"));

        // Verify the failure when the value is min - 1d or max + 1d
        assertThatThrownBy(() ->
                SqlDataTypeTest.create()
                        .addRoundTrip("DATE", "DATE '1699-12-31'", DateType.DATE, "DATE '1699-12-31'")
                        .execute(getQueryRunner(), salesforceCreateAndInsert("test_date")))
                .hasMessageContaining("INSERT INTO")
                .hasStackTraceContaining("invalid date");
        assertThatThrownBy(() ->
                SqlDataTypeTest.create()
                        .addRoundTrip("DATE", "DATE '4001-01-01'", DateType.DATE, "DATE '4001-01-01'")
                        .execute(getQueryRunner(), salesforceCreateAndInsert("test_date")))
                .hasMessageContaining("INSERT INTO")
                .hasStackTraceContaining("invalid date");
    }

    @Test(dataProvider = "testTimestampDataProvider")
    public void testTime(ZoneId sessionZone)
    {
        // TIME data types inserted via the Salesforce JDBC driver do not support the test time zone
        // When read back the times are +1 hour
        throw new SkipException("TIME data type tests are skipped");
    }

    @Test(dataProvider = "testTimestampDataProvider")
    public void testTimestamp(ZoneId sessionZone)
    {
        SqlDataTypeTest tests = SqlDataTypeTest.create();

        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        tests.addRoundTrip("TIMESTAMP", "TIMESTAMP '1958-01-01 13:18:03'", TIMESTAMP_SECONDS, "CAST(TIMESTAMP '1958-01-01 13:18:03' AS TIMESTAMP(0))");
        tests.addRoundTrip("TIMESTAMP", "TIMESTAMP '2019-03-18 01:17:00'", TIMESTAMP_SECONDS, "CAST(TIMESTAMP '2019-03-18 01:17:00' AS TIMESTAMP(0))");
        tests.addRoundTrip("TIMESTAMP", "TIMESTAMP '2018-10-28 01:33:17'", TIMESTAMP_SECONDS, "CAST(TIMESTAMP '2018-10-28 01:33:17' AS TIMESTAMP(0))");
        tests.addRoundTrip("TIMESTAMP", "TIMESTAMP '2018-10-28 03:33:33'", TIMESTAMP_SECONDS, "CAST(TIMESTAMP '2018-10-28 03:33:33' AS TIMESTAMP(0))");
        // tests.addRoundTrip("TIMESTAMP", "TIMESTAMP '1970-01-01 00:00:00'", TIMESTAMP_SECONDS, "CAST(TIMESTAMP '1970-01-01 00:00:00' AS TIMESTAMP(0))"); // "TIMESTAMP '1970-01-01 00:00:00'" also is a gap in JVM zone
        // tests.addRoundTrip("TIMESTAMP", "TIMESTAMP '1970-01-01 00:13:42'", TIMESTAMP_SECONDS, "CAST(TIMESTAMP '1970-01-01 00:13:42' AS TIMESTAMP(0))"); // expected [1970-01-01T00:13:42] but found [1970-01-01T01:13:42]
        // tests.addRoundTrip("TIMESTAMP", "TIMESTAMP '2018-04-01 02:13:55'", TIMESTAMP_SECONDS, "CAST(TIMESTAMP '2018-04-01 02:13:55' AS TIMESTAMP(0))"); // [2018-04-01T02:13:55] but found [2018-04-01T03:13:55]
        tests.addRoundTrip("TIMESTAMP", "TIMESTAMP '2018-03-25 03:17:17'", TIMESTAMP_SECONDS, "CAST(TIMESTAMP '2018-03-25 03:17:17' AS TIMESTAMP(0))");
        tests.addRoundTrip("TIMESTAMP", "TIMESTAMP '1986-01-01 00:13:07'", TIMESTAMP_SECONDS, "CAST(TIMESTAMP '1986-01-01 00:13:07' AS TIMESTAMP(0))");

        tests.execute(getQueryRunner(), session, salesforceCreateAndInsert("test_timestamp"));
    }

    @DataProvider
    public Object[][] testTimestampDataProvider()
    {
        return new Object[][] {
                {UTC},
                {jvmZone},
                {vilnius},
                {kathmandu},
                {ZoneId.of(TestingSession.DEFAULT_TIME_ZONE_KEY.getId())},
        };
    }

    private DataSetup salesforceCreateAndInsert(String tableName)
    {
        return new SalesforceCreateAndInsertDataSetup(new JdbcSqlExecutor(jdbcUrl), jdbcUrl, tableName);
    }

    private static void checkIsGap(ZoneId zone, LocalDateTime dateTime)
    {
        verify(isGap(zone, dateTime), "Expected %s to be a gap in %s", dateTime, zone);
    }

    private static boolean isGap(ZoneId zone, LocalDateTime dateTime)
    {
        return zone.getRules().getValidOffsets(dateTime).isEmpty();
    }

    private static void checkIsDoubled(ZoneId zone, LocalDateTime dateTime)
    {
        verify(zone.getRules().getValidOffsets(dateTime).size() == 2, "Expected %s to be doubled in %s", dateTime, zone);
    }
}

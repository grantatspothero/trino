/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake.jdbc;

import com.starburstdata.trino.plugins.snowflake.SnowflakeSessionProperties;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.type.Type;

import java.util.Optional;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.expression;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static java.lang.String.format;

public class RewriteJsonExtract
        implements ConnectorExpressionRule<Call, String>
{
    private static final Capture<ConnectorExpression> VALUE = newCapture();
    private static final Capture<ConnectorExpression> JSON_PATH = newCapture();

    private final Pattern<Call> pattern;

    public RewriteJsonExtract(Type jsonType)
    {
        this.pattern = call()
                .with(functionName().equalTo(new FunctionName("json_extract")))
                .with(type().equalTo(jsonType))
                .with(argumentCount().equalTo(2))
                .with(argument(0).matching(expression().capturedAs(VALUE)))
                // this only captures cases where the JSON_PATH is a literal and no CAST is involved
                .with(argument(1).matching(expression().capturedAs(JSON_PATH)));
    }

    @Override
    public Pattern<Call> getPattern()
    {
        return pattern;
    }

    @Override
    public Optional<String> rewrite(Call call, Captures captures, RewriteContext<String> context)
    {
        if (!SnowflakeSessionProperties.getExperimentalPushdownEnabled(context.getSession())) {
            return Optional.empty();
        }

        Optional<String> value = context.defaultRewrite(captures.get(VALUE));
        if (value.isEmpty()) {
            return Optional.empty();
        }

        Optional<String> jsonPath = context.defaultRewrite(captures.get(JSON_PATH));
        if (jsonPath.isEmpty()) {
            return Optional.empty();
        }

        String snowflakeGetPath = format("GET_PATH((%s), (%s))", value.get(), jsonPath.get());
        return Optional.of(snowflakeGetPath);
    }
}
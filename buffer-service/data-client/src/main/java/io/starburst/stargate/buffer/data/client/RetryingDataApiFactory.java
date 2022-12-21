/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.client;

import io.airlift.units.Duration;

import javax.inject.Inject;

import java.net.URI;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.Objects.requireNonNull;

public class RetryingDataApiFactory
        implements DataApiFactory
{
    private final DataApiFactory dataApiFactoryDelegate;
    private final ScheduledExecutorService executorService;
    private final int maxRetries;
    private final Duration backoffInitial;
    private final Duration backoffMax;
    private final double backoffFactor;
    private final double backoffJitter;

    @Inject
    public RetryingDataApiFactory(
            DataApiConfig config,
            @ForRetryingDataApiFactory DataApiFactory dataApiFactoryDelegate,
            @ForRetryingDataApiFactory ScheduledExecutorService executorService)
    {
        this.dataApiFactoryDelegate = requireNonNull(dataApiFactoryDelegate, "dataApiFactoryDelegate is null");
        this.executorService = executorService;
        this.maxRetries = config.getDataClientMaxRetries();
        this.backoffInitial = config.getDataClientRetryBackoffInitial();
        this.backoffMax = config.getDataClientRetryBackoffMax();
        this.backoffFactor = config.getDataClientRetryBackoffFactor();
        this.backoffJitter = config.getDataClientRetryBackoffJitter();
    }

    @Override
    public DataApi createDataApi(URI baseUri, long targetBufferNodeId)
    {
        DataApi dataApiDelegate = dataApiFactoryDelegate.createDataApi(baseUri, targetBufferNodeId);
        return new RetryingDataApi(dataApiDelegate, maxRetries, backoffInitial, backoffMax, backoffFactor, backoffJitter, executorService);
    }
}

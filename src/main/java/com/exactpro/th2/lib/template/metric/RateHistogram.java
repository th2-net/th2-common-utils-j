package com.exactpro.th2.lib.template.metric;

import io.prometheus.client.Histogram;

import java.io.Closeable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public abstract class RateHistogram implements Closeable {

    protected final ScheduledFuture<?> scheduler;
    protected final Histogram histogram;

    protected RateHistogram(ScheduledExecutorService scheduledExecutorService,
                                       Histogram prometheusHistogram,
                                       long intervalMs) {
        this.histogram = prometheusHistogram;
        this.scheduler = scheduledExecutorService.scheduleAtFixedRate(this::observeRates, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    protected abstract void observeRates();

    @Override
    public void close() {
        scheduler.cancel(true);
    }
}

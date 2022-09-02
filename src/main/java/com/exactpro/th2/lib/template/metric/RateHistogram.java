/*
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.lib.template.metric;

import io.prometheus.client.Histogram;
import io.prometheus.client.Histogram.Child;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;


public abstract class RateHistogram<T> implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RateHistogram.class.getName());

    protected final ScheduledFuture<?> scheduler;
    protected final Histogram histogram;
    protected final ConcurrentMap<T, HistogramCounterImpl> labelCounterMap = new ConcurrentHashMap<>();


    protected RateHistogram(ScheduledExecutorService scheduledExecutorService,
                                       Histogram prometheusHistogram,
                                       long observationIntervalMs) {
        this.histogram = prometheusHistogram;
        this.scheduler = scheduledExecutorService.scheduleAtFixedRate(this::observeRates, observationIntervalMs, observationIntervalMs, TimeUnit.MILLISECONDS);
    }

    protected void observeRates() {
        labelCounterMap.forEach((label, histogramCounter) -> {
            try {
                histogramCounter.observe();
            } catch (Exception e) {
                LOGGER.error("Failed to observe rate for labels: {}", label, e);
            }
        });
    }


    public void inc(long value, T label) {
        getOrCreateCounter(label).increment(value);
    }

    public void inc(T label) {
        inc(1L, label);
    }

    /**
     * Be sure to provide exact the same amount of labels that was provided for io.prometheus.client.Histogram creation. <br>
     * IllegalArgumentException will be thrown otherwise.
     *
     * @param label label set for counter
     * @return new or existed HistogramCounter for label set
     */
    public HistogramCounter getOrCreateCounter(T label) {
        return labelCounterMap.computeIfAbsent(label, newLabels -> new HistogramCounterImpl(getHistogramChild(label)));
    }

    /**
     * We need to specify a way to get String or String[] from the generic type
     *
     * @implNote example: return histogram.labels(T -> String | String[])
     * @param label generic key for HistogramCounter map
     * @return corresponding Histogram.Child object
     */
    protected abstract Child getHistogramChild(T label);

    @Override
    public void close() {
        scheduler.cancel(true);
    }
}

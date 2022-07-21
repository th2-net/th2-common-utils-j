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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

public class RateHistogram implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RateHistogram.class.getName());

    private final ScheduledFuture<?> fixedRate;
    private final Histogram histogram;
    private final ConcurrentMap<String, HistogramCounter> counterMap = new ConcurrentHashMap<>();

    public RateHistogram(ScheduledExecutorService scheduledExecutorService,
                         Histogram prometheusHistogram,
                         long intervalMs) {
        this.histogram = prometheusHistogram;

        this.fixedRate = scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                counterMap.forEach((key, histogramCounter) ->
                        histogramCounter.getHistogramChild().observe(histogramCounter.getCounter().sumThenReset()));
            } catch (Exception e) {
                LOGGER.error("Error inside scheduled metric retrieving: ", e);
            }
        }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    public void inc(Collection<String> labels, long value) {
        labels.forEach(label -> inc(label, value));
    }

    public void inc(String label, long value) {
        var histogramCounter = counterMap.computeIfAbsent(label, key -> new HistogramCounter(
                histogram.labels(key),
                new LongAdder()));
        histogramCounter.getCounter().add(value);
    }

    @Override
    public void close() {
        fixedRate.cancel(true);
    }
}

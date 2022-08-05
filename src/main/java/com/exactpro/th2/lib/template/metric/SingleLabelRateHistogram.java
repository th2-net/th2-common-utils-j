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

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;

/**
 * This class is used for periodically update Prometheus metrics. <br> <br>
 * Be aware that you must use the <strong> same amount of labels </strong> for counter creation that you used to create Histogram instance.
 * This class is made to use only one label.
 * Use {@link MultiLabelRateHistogram} if amount of labels isn't 1.
 */
public class SingleLabelRateHistogram extends RateHistogram {
    private static final Logger LOGGER = LoggerFactory.getLogger(SingleLabelRateHistogram.class.getName());

    private final ConcurrentMap<String, HistogramCounter> singleLabelCounterMap = new ConcurrentHashMap<>();

    protected SingleLabelRateHistogram(ScheduledExecutorService scheduledExecutorService, Histogram prometheusHistogram, long intervalMs) {
        super(scheduledExecutorService, prometheusHistogram, intervalMs);
    }

    @Override
    protected void observeRates() {
        singleLabelCounterMap.forEach((label, histogramCounter) -> {
            try {
                histogramCounter.observe();
            } catch (Exception e) {
                LOGGER.error("Failed to observe rate for single label: {}", label, e);
            }
        });
    }

    public void incAll(Collection<String> labels, long value) {
        labels.forEach(label -> inc(label, value));
    }

    public void inc(String label, long value) {
        getCounterForLabel(label)
                .increment(value);
    }

    public void inc(String label) {
        inc(label, 1L);
    }

    public HistogramCounter getCounterForLabel(String label) {
        return singleLabelCounterMap.computeIfAbsent(label, newLabel -> new HistogramCounter(histogram.labels(newLabel)));
    }

}

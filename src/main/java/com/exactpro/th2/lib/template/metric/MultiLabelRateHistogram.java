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
import java.util.stream.Collectors;

/**
 * This class is used for periodically update Prometheus metrics. <br> <br>
 * Be aware that you must use the <strong>  same amount of labels </strong> for counter creation that you used to create Histogram instance.
 * This number is final and cannot be changed. <br> <br>
 * Use {@link SingleLabelRateHistogram} if amount of labels is 1.
 */
public class MultiLabelRateHistogram extends RateHistogram {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiLabelRateHistogram.class.getName());

    private final ConcurrentMap<Collection<String>, HistogramCounter> multiLabelCounterMap = new ConcurrentHashMap<>();

    public MultiLabelRateHistogram(ScheduledExecutorService scheduledExecutorService, Histogram prometheusHistogram, long intervalMs) {
        super(scheduledExecutorService, prometheusHistogram, intervalMs);
    }

    @Override
    protected void observeRates() {
        multiLabelCounterMap.forEach((labels, histogramCounter) -> {
            try {
                histogramCounter.observe();
            } catch (Exception e) {
                LOGGER.error("Failed to observe rate for multi label: {}", labels.stream().collect(Collectors.joining(",", "[", "]")), e);
            }
        });
    }

    public void inc(Collection<String> labels, long value) {
        getCounterForLabels(labels)
                .increment(value);
    }

    public void inc(Collection<String> labels) {
        inc(labels, 1L);
    }

    /**
     * Be sure to provide exact the same amount of labels that was provided for io.prometheus.client.Histogram creation. <br>
     * IllegalArgumentException will be thrown otherwise.
     *
     * @param labels label set for counter
     * @return new or existed HistogramCounter for label set
     */
    public HistogramCounter getCounterForLabels(Collection<String> labels) {
        return multiLabelCounterMap.computeIfAbsent(labels, newLabels -> new HistogramCounter(histogram.labels(newLabels.toArray(new String[0]))));
    }

}

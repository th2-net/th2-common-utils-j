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

import java.util.concurrent.ScheduledExecutorService;

/**
 * This class is used for periodically update Prometheus metrics. <br> <br>
 * Be aware that you must use the <strong> same amount of labels </strong> for counter creation that you used to create Histogram instance.
 * This class is made to use only one label.
 * Use {@link MultiLabelRateHistogram} if amount of labels isn't 1.
 */
public class SingleLabelRateHistogram extends RateHistogram<String> {

    protected SingleLabelRateHistogram(ScheduledExecutorService scheduledExecutorService, Histogram prometheusHistogram, long intervalMs) {
        super(scheduledExecutorService, prometheusHistogram, intervalMs);
    }

    @Override
    protected Child getHistogramChild(String label) {
        return histogram.labels(label);
    }

}

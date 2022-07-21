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

import java.util.concurrent.atomic.LongAdder;

public class HistogramCounter {
    private final Histogram.Child histogramChild;
    private final LongAdder counter;

    public HistogramCounter(Histogram.Child histogramChild, LongAdder counter) {
        this.histogramChild = histogramChild;
        this.counter = counter;
    }

    public Histogram.Child getHistogramChild() {
        return histogramChild;
    }

    public LongAdder getCounter() {
        return counter;
    }
}

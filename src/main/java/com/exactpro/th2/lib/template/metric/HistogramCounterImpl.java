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

import io.prometheus.client.Histogram.Child;

import java.util.concurrent.atomic.LongAdder;

public class HistogramCounterImpl implements HistogramCounter {
    private final Child histogramChild;
    private final LongAdder counter = new LongAdder();

    public HistogramCounterImpl(Child histogramChild) {
        this.histogramChild = histogramChild;
    }

    public void increment(long value) {
        counter.add(value);
    }
    public void observe() {
        histogramChild.observe(counter.sumThenReset());
    }
}

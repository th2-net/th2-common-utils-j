package com.exactpro.th2.lib.template.metric;

import io.prometheus.client.Histogram;
import kotlin.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

public class PrometheusMetricUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(PrometheusMetricUtils.class.getName());

    private final ScheduledExecutorService scheduler;
    private final Histogram histogram;
    private final ConcurrentMap<String, Pair<Histogram.Child, LongAdder>> counterMap = new ConcurrentHashMap<>();

    public PrometheusMetricUtils(ScheduledExecutorService scheduledExecutorService,
                                 Histogram prometheusHistogram,
                                 long intervalMs) {
        this.scheduler = scheduledExecutorService;
        this.histogram = prometheusHistogram;

        scheduler.scheduleAtFixedRate(() -> {
            try {
                counterMap.forEach((key, value) ->
                        value.getFirst().observe(value.getSecond().sumThenReset()));
            } catch (Exception e) {
                LOGGER.error("Error inside scheduled metric retrieving: ", e);
            }
        }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    public void inc(String label, long value) {
        var pair = counterMap.computeIfAbsent(label, key -> new Pair<>(
                histogram.labels(key),
                new LongAdder()));
        pair.getSecond().add(value);
    }

    public void inc(String label) {
        var pair = counterMap.computeIfAbsent(label, key -> new Pair<>(
                histogram.labels(key),
                new LongAdder()));
        pair.getSecond().increment();
    }

}

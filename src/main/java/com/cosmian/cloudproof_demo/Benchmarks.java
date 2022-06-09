package com.cosmian.cloudproof_demo;

import java.io.Serializable;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Ths class records timings for benchmarks
 */
public class Benchmarks implements Serializable {

    class Values implements Serializable {
        final double count;

        final double sum;

        public Values(double count, double sum) {
            this.count = count;
            this.sum = sum;
        }

    }

    private final ConcurrentHashMap<String, Values> records = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, Long> recorder = new ConcurrentHashMap<>();

    public Benchmarks() {

    }

    /**
     * Start recording a bench event
     * 
     * @param bench the bench name
     */
    public void startRecording(String bench) {
        recorder.put(bench, System.nanoTime());
    }

    /**
     * Stop recording a bench event started with {@link #startRecording(String)} and record it
     * 
     * @param bench the bench name
     * @return the bench event duration in µs
     */
    public long stopRecording(String bench, long numEvents) {
        long end = System.nanoTime();
        Long start = recorder.get(bench);
        if (start == null) {
            return 0;
        }
        long micros = TimeUnit.NANOSECONDS.toMicros(end - start);
        record(bench, numEvents, micros);
        return micros;
    }

    /**
     * Record a value for on event the current bench
     * 
     * @param bench bench name
     * @param value value in µs
     */
    public void record(String bench, long numEvents, long value) {
        records.compute(bench, (key, current) -> {
            if (current == null) {
                return new Values((double) numEvents, (double) value);
            }
            double newCount = current.count + (double) numEvents;
            double newSum = current.sum + (double) value;
            return new Values(newCount, newSum);
        });
    }

    public long getCount(String bench) {
        Values v = this.records.get(bench);
        return v == null ? 0 : (long) v.count;
    }

    public long getAverage(String bench) {
        Values v = this.records.get(bench);
        return v == null ? 0 : Math.round(v.sum / v.count);
    }

    public double getSum(String bench) {
        Values v = this.records.get(bench);
        return v == null ? 0 : v.sum;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Benchmarks:\n");
        for (Entry<String, Values> entry : this.records.entrySet()) {
            builder.append(entry.getKey() + ": ");
            builder.append("count: " + String.format("%,.0f", entry.getValue().count));
            builder.append(", total: " + String.format("%,.0f", entry.getValue().sum) + "µs");
            builder
                .append(", average: " + String.format("%,.0f", entry.getValue().sum / entry.getValue().count) + "µs");
            builder.append("\n");
        }
        return builder.toString();
    }

}

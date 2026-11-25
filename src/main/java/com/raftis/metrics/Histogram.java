package com.raftis.metrics;

/**
 * A Prometheus-style histogram of durations: how many observations fell at or under each bucket
 * boundary, plus their count and sum. Not thread-safe: each one is only touched by the event loop.
 */
public final class Histogram {
    /** Bucket upper bounds in seconds, from 0.1 ms to 5 s: wide enough for both fsyncs and elections. */
    private static final double[] BOUNDS = {0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05,
            0.1, 0.25, 0.5, 1, 2.5, 5};

    private final long[] counts = new long[BOUNDS.length];
    private long count;
    private double sum;

    public void observeNanos(long nanos) {
        double seconds = nanos / 1e9;
        for (int i = 0; i < BOUNDS.length; i++) {
            if (seconds <= BOUNDS[i]) {
                counts[i]++;
                break;
            }
        }
        count++;
        sum += seconds;
    }

    public long count() {
        return count;
    }

    void writeTo(StringBuilder sb, String name, String labels) {
        long cumulative = 0;
        String sep = labels.isEmpty() ? "" : labels + ",";
        for (int i = 0; i < BOUNDS.length; i++) {
            cumulative += counts[i];
            sb.append(name).append("_bucket{").append(sep).append("le=\"")
                    .append(java.math.BigDecimal.valueOf(BOUNDS[i]).toPlainString()).append("\"} ")
                    .append(cumulative).append('\n');
        }
        sb.append(name).append("_bucket{").append(sep).append("le=\"+Inf\"} ").append(count).append('\n');
        String braces = labels.isEmpty() ? "" : "{" + labels + "}";
        sb.append(name).append("_sum").append(braces).append(' ').append(sum).append('\n');
        sb.append(name).append("_count").append(braces).append(' ').append(count).append('\n');
    }
}

package com.raftis.metrics;

/**
 * Builds the Prometheus text exposition format: for each metric a {@code # HELP} and
 * {@code # TYPE} line, then one line per value, e.g. {@code raftis_raft_term 7}.
 */
public final class PrometheusText {
    private final StringBuilder sb = new StringBuilder(4096);

    public PrometheusText gauge(String name, String help, double value) {
        header(name, help, "gauge");
        return sample(name, "", value);
    }

    public PrometheusText counter(String name, String help, double value) {
        header(name, help, "counter");
        return sample(name, "", value);
    }

    /** Starts a metric whose values have labels; add them with {@link #sample}. */
    public PrometheusText header(String name, String help, String type) {
        sb.append("# HELP ").append(name).append(' ').append(help).append('\n');
        sb.append("# TYPE ").append(name).append(' ').append(type).append('\n');
        return this;
    }

    /** One value line; {@code labels} looks like {@code peer="2"} (or is empty). */
    public PrometheusText sample(String name, String labels, double value) {
        sb.append(name);
        if (!labels.isEmpty()) sb.append('{').append(labels).append('}');
        sb.append(' ');
        if (value == Math.rint(value) && !Double.isInfinite(value)) sb.append((long) value);
        else sb.append(value);
        sb.append('\n');
        return this;
    }

    public PrometheusText histogram(String name, String help, Histogram h) {
        header(name, help, "histogram");
        h.writeTo(sb, name, "");
        return this;
    }

    @Override
    public String toString() {
        return sb.toString();
    }
}

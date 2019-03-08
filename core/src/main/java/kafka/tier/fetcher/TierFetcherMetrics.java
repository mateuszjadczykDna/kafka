package kafka.tier.fetcher;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.Value;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class TierFetcherMetrics {
    private final Metrics metrics;
    private final String metricGroupName = "tier.fetcher";


    private final String bytesFetchedPrefix = "bytes-fetched";
    final MetricName bytesFetchedRateMetricName;
    final MetricName bytesFetchedTotalMetricName;
    private final Sensor bytesFetched;

    private final String inFlightPrefix = "in-flight";
    final MetricName inFlightValueMetricName;
    private final Sensor inFlight;

    private final List<Sensor> sensors = new ArrayList<>();

    TierFetcherMetrics(Metrics metrics) {
        this.metrics = metrics;

        this.bytesFetched = sensor(bytesFetchedPrefix);
        this.bytesFetchedRateMetricName = metrics.metricName(bytesFetchedPrefix +
                "-rate", metricGroupName, "The number of bytes fetched per second from tiered "
                + "storage", Collections.emptyMap());
        this.bytesFetchedTotalMetricName = metrics.metricName(bytesFetchedPrefix +
                "-total", metricGroupName, "The total number of bytes fetched from tiered "
                + "storage", Collections.emptyMap());
        final Meter bytesFetchedMeter = new Meter(bytesFetchedRateMetricName,
                bytesFetchedTotalMetricName);
        this.bytesFetched.add(bytesFetchedMeter);

        this.inFlight = sensor(inFlightPrefix);
        this.inFlightValueMetricName = metrics.metricName(inFlightPrefix +
                "-value", metricGroupName, "The current estimated number of in-flight fetches "
                + "going to tiered storage", Collections.emptyMap());
        this.inFlight.add(inFlightValueMetricName, new Value());
    }

    private Sensor sensor(String name, Sensor... parents) {
        Sensor sensor = metrics.sensor(name, parents);
        sensors.add(sensor);
        return sensor;
    }

    public void close() {
        for (Sensor sensor : sensors)
            metrics.removeSensor(sensor.name());
    }

    public void recordBytesFetched(int bytes) {
        this.bytesFetched.record(bytes);
    }

    public void setNumInFlight(long numInFlight) {
        this.inFlight.record(numInFlight);
    }

}

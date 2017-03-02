package io.deepsense.neptune;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.deepsense.neptune.apiclient.ApiException;
import io.deepsense.neptune.clientlibrary.models.*;
import io.deepsense.neptune.clientlibrary.models.impl.charts.ChartSeriesCollectionImpl;
import io.deepsense.neptune.clientlibrary.models.impl.context.NeptuneContextBuilderFactory;
import io.deepsense.neptune.clientlibrary.parsers.jobargumentsparser.JobArguments;
import io.deepsense.neptune.service.NeptuneService;

import java.net.URI;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class NeptuneReporter extends ScheduledReporter {

    private static final String PROTOCOL_PREFIX = "https://";

    private final Job job;

    private final Map<String, Channel<Double>> numericChannels;

    private final Map<String, Chart> charts;


    public NeptuneReporter(MetricRegistry registry, String name, MetricFilter filter, TimeUnit rateUnit,
                           TimeUnit durationUnit, ScheduledExecutorService executor, boolean shutdownExecutorOnStop,
                           Set<MetricAttribute> disabledMetricAttributes) {
        super(registry, name, filter, rateUnit, durationUnit, executor, shutdownExecutorOnStop,
                disabledMetricAttributes);

        final String neptuneHost = getNeptuneHost();
        final String neptuneUser = getNeptuneUser();
        final String neptunePassword = getNeptunePassword();
        final URI neptuneURI = URI.create(PROTOCOL_PREFIX + neptuneHost);
        final NeptuneService neptuneService = new NeptuneService(neptuneURI, neptuneUser, neptunePassword);
        final UUID createdExpId = createNeptuneExperiment(name, neptuneService);

        System.out.println("Created Exp: " + createdExpId);

        Runtime.getRuntime().addShutdownHook(new Thread(new ClosingThread(neptuneService, createdExpId)));

        final URI wsUri = URI.create("ws://" + neptuneHost);
        final JobArguments jobArguments = new JobArguments(neptuneURI, wsUri, createdExpId, false);
        NeptuneContext neptuneContext = null;
        try {
            neptuneContext = new NeptuneContextBuilderFactory().create(jobArguments).build();
        } catch (ApiException e) {
            e.printStackTrace();
        }
        neptuneService.markJobAsExecuting(createdExpId);
        this.job = neptuneContext.getJob();
        this.numericChannels = new HashMap<>();
        this.charts = new HashMap<>();
    }

    private UUID createNeptuneExperiment(String name, NeptuneService neptuneService) {
        return neptuneService.createExperiment(name, name, name);
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {
        final double now = System.nanoTime();

        processGauges(gauges, now);
        processCounters(counters, now);
        processMeters("meter_", new TreeMap<>(meters), now);
        processTimers(timers, now);

    }

    private void processTimers(SortedMap<String, Timer> timers, double now) {
        final String prefix = "timer_";
        processMeters(prefix, new TreeMap<>(timers), now);
        processSnapshots(prefix, timers, now);
    }

    private void processSnapshots(String prefix, SortedMap<String, Timer> timers, double now) {
        for (Map.Entry<String, Timer> entry: timers.entrySet()) {
            final String timerName = prefix + entry.getKey();
            final Snapshot snapshot = entry.getValue().getSnapshot();
            final double p75 = snapshot.get75thPercentile();
            final double p95 = snapshot.get95thPercentile();
            final double p98 = snapshot.get98thPercentile();
            final double p99 = snapshot.get99thPercentile();
            final double p999 = snapshot.get999thPercentile();
            final double max = snapshot.getMax();
            final double min = snapshot.getMin();
            final double mean = snapshot.getMean();
            final double median = snapshot.getMedian();
            final double stdDev = snapshot.getStdDev();

            final Channel<Double> p75Channel = sendValueToChannel(timerName + "_p75", now, p75);
            final Channel<Double> p95Channel = sendValueToChannel(timerName + "_p95", now, p95);
            final Channel<Double> p98Channel = sendValueToChannel(timerName + "_p98", now, p98);
            final Channel<Double> p99Channel = sendValueToChannel(timerName + "_p99", now, p99);
            final Channel<Double> p999Channel = sendValueToChannel(timerName + "_p909", now, p999);
            final Channel<Double> maxChannel = sendValueToChannel(timerName + "_max", now, max);
            final Channel<Double> minChannel = sendValueToChannel(timerName + "_min", now, min);
            final Channel<Double> meanChannel = sendValueToChannel(timerName + "_mean", now, mean);
            final Channel<Double> medianChannel = sendValueToChannel(timerName + "_median", now, median);
            final Channel<Double> stdDevChannel = sendValueToChannel(timerName + "_stdDev", now, stdDev);

            charts.computeIfAbsent(timerName + "_distribution",
                    key -> createChart(key,
                            ImmutableList.of(
                                    p75Channel,
                                    p95Channel,
                                    p98Channel,
                                    p99Channel,
                                    p999Channel,
                                    maxChannel,
                                    minChannel,
                                    meanChannel,
                                    medianChannel,
                                    stdDevChannel
                            )
                    )
            );
        }
    }


    private void processMeters(String prefix, SortedMap<String, Metered> meters, double now) {
        for (Map.Entry<String, Metered> entry : meters.entrySet()) {
            final String meterName = prefix + entry.getKey();
            final long count = entry.getValue().getCount();
            final double fifteenMinutesRate = entry.getValue().getFifteenMinuteRate();
            final double fiveMinutesRate = entry.getValue().getFiveMinuteRate();
            final double oneMinuteRate = entry.getValue().getFiveMinuteRate();
            final double meanRate = entry.getValue().getMeanRate();

            final Channel<Double> countChannel =
                    sendValueToChannel(meterName + "_count", now, (double)count);
            final Channel<Double> fifteenMinutesRateChannel =
                    sendValueToChannel(meterName + "_15M", now, fifteenMinutesRate);
            final Channel<Double> fiveMinutesRateChannel =
                    sendValueToChannel(meterName + "_5M", now, fiveMinutesRate);
            final Channel<Double> oneMinutesRateChannel =
                    sendValueToChannel(meterName + "_1M", now, oneMinuteRate);
            final Channel<Double> meanRateChannel =
                    sendValueToChannel(meterName + "_mean", now, meanRate);

            charts.computeIfAbsent(meterName + "_count", key -> createChart(key, ImmutableList.of(countChannel)));
            charts.computeIfAbsent(meterName + "_rates",
                    key -> createChart(key,
                            ImmutableList.of(
                                    fifteenMinutesRateChannel,
                                    fiveMinutesRateChannel,
                                    oneMinutesRateChannel,
                                    meanRateChannel
                            )
                    )
            );
        }
    }

    private void processCounters(SortedMap<String, Counter> counters, double now) {
        for (Map.Entry<String, Counter> entry : counters.entrySet()) {
            final long counterValue = entry.getValue().getCount();
            final String counterName = getCounterName(entry.getKey());
            final Channel<Double> channel = sendValueToChannel(counterName, now, (double)counterValue);
            charts.computeIfAbsent(counterName, key -> createChart(key, ImmutableList.of(channel)));
        }
    }

    private void processGauges(SortedMap<String, Gauge> gauges, double now) {
        for (Map.Entry<String, Gauge> entry: gauges.entrySet()) {
            final Object value = entry.getValue();
            if (value instanceof Number) {
                final Number numberValue = (Number)value;
                final String gaugeName = getGaugeName(entry.getKey());
                final Channel<Double> channel = sendValueToChannel(gaugeName, now, numberValue.doubleValue());
                charts.computeIfAbsent(gaugeName, key -> createChart(key, ImmutableList.of(channel)));
            }
        }
    }

    private Channel<Double> sendValueToChannel(String channelName, double x, double y) {
        final Channel<Double> channel = getOrCreateChannel(channelName);
        channel.send(x, y);
        return channel;
    }

    private Channel<Double> getOrCreateChannel(String channelName) {
        return numericChannels.computeIfAbsent(channelName, k -> job.createNumericChannel(channelName));
    }

    private Chart createChart(String name, Collection<Channel<Double>> channels) {
        final ChartSeriesCollection chartSeriesCollection = new ChartSeriesCollectionImpl();
        channels.forEach(chartSeriesCollection::add);
        return job.createChart(name, chartSeriesCollection);
    }

    private String getGaugeName(String gauge) {
        return "gauge_" + gauge;
    }

    private String getCounterName(String counter) {
        return "counter_" + counter;
    }

    private static String getNeptuneHost() {
        final String neptuneHost = getEnvVar("NEPTUNE_HOST");
        if (neptuneHost.startsWith(PROTOCOL_PREFIX)) {
            return neptuneHost.substring(PROTOCOL_PREFIX.length());
        }
        return neptuneHost;
    }

    private static String getNeptuneUser() {
        return getEnvVar("NEPTUNE_USER");
    }

    private static String getNeptunePassword() {
        return getEnvVar("NEPTUNE_PASSWORD");
    }

    private static String getEnvVar(String varKey) {
        return Preconditions.checkNotNull(System.getenv(varKey),
                "You need to set " + varKey + " environment variable");
    }

}

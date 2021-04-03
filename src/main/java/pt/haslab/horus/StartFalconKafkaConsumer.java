package pt.haslab.horus;

import com.codahale.metrics.*;
import com.codahale.metrics.jmx.JmxReporter;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

public class StartFalconKafkaConsumer {
    public static void main(String[] args)
            throws Exception {
        String kafkaServers = Config.getInstance().getConfig().getString("kafka.servers");
        String[] kafkaTopics = Config.getInstance().getConfig().getStringArray("kafka.topics");

        MetricRegistry registry = SharedMetricRegistries.getOrCreate("stats");
        List<ScheduledReporter> reporters = new ArrayList<ScheduledReporter>();

        if (Config.getInstance().getConfig().getBoolean("reporter.stdout", false)) {
            final ConsoleReporter consoleReporter = ConsoleReporter.forRegistry(registry)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build();
            consoleReporter.start(30, TimeUnit.SECONDS);
            reporters.add(consoleReporter);
        }

        if (Config.getInstance().getConfig().getBoolean("reporter.jmx", true)) {
            final JmxReporter jmxReporter = JmxReporter.forRegistry(registry).build();

            jmxReporter.start();
        }

        if (Config.getInstance().getConfig().getBoolean("reporter.csv", false)) {
            final CsvReporter csvReporter = CsvReporter.forRegistry(registry)
                    .formatFor(Locale.ENGLISH)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build(new File(Config.getInstance().getConfig().getString("reporter.csv.output")));
            csvReporter.start(Config.getInstance().getConfig().getInt("reporter.csv.period", 10), TimeUnit.SECONDS);
            reporters.add(csvReporter);
        }

        (new FalconConsumer(kafkaServers, Arrays.asList(kafkaTopics))).run();

        reporters.forEach(ScheduledReporter::stop);
    }
}

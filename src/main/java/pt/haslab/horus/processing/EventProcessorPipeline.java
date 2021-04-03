package pt.haslab.horus.processing;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.conversantmedia.util.concurrent.PushPullBlockingQueue;
import com.conversantmedia.util.concurrent.SpinPolicy;
import org.apache.commons.configuration.Configuration;
import pt.haslab.horus.FalconEventHandler;
import pt.haslab.horus.events.Event;
import pt.haslab.horus.graph.ExecutionGraph;
import pt.haslab.horus.processing.processors.EventHappensBeforeRelationProcessor;
import pt.haslab.horus.processing.processors.TimelineEventAppenderProcessor;
import pt.haslab.horus.util.NamedThreadFactory;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.*;

import static com.codahale.metrics.MetricRegistry.name;

public class EventProcessorPipeline implements AutoCloseable, EventProcessor {
    private final ExecutionGraph graph;
    private BlockingQueue<Event> appendedEvents;
    private TimelineEventAppenderProcessor appenderProcessor;
    private EventHappensBeforeRelationProcessor happensBeforeProcessor;
    private ExecutorService asyncProcessorExecutors = Executors.newSingleThreadExecutor(new NamedThreadFactory("hb-processor-"));
    private boolean processorsRunning = false;
    private Configuration configuration;
    private final MetricRegistry statsRegistry = SharedMetricRegistries.getOrCreate("stats");

    public EventProcessorPipeline(ExecutionGraph graph, Configuration configuration) {
        this.appendedEvents = new ArrayBlockingQueue<>(configuration.getInt("async.queueSize", 5000));
        this.appenderProcessor = new TimelineEventAppenderProcessor(graph, appendedEvents, configuration);
        this.configuration = configuration;
        this.graph = graph;
    }

    @Override
    public void process(Event event) {
        if (!this.processorsRunning) {
            this.launchProcessors();
        }

        this.appenderProcessor.process(event);
    }

    private void launchProcessors() {
        this.happensBeforeProcessor = new EventHappensBeforeRelationProcessor(this.graph, appendedEvents, configuration);
        this.asyncProcessorExecutors.submit(this.happensBeforeProcessor);

        this.statsRegistry.register(name(EventProcessorPipeline.class, "queue-size"), new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return appendedEvents.size();
            }
        });

        this.processorsRunning = true;
    }

    @Override
    public void close() throws Exception {
        this.appenderProcessor.close();
        this.happensBeforeProcessor.close();
        this.asyncProcessorExecutors.shutdown();
        this.asyncProcessorExecutors.awaitTermination(60, TimeUnit.SECONDS);
    }
}


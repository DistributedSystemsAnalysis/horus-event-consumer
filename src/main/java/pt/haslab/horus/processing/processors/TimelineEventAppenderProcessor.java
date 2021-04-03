package pt.haslab.horus.processing.processors;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.haslab.horus.events.Event;
import pt.haslab.horus.graph.ExecutionGraph;
import pt.haslab.horus.graph.timeline.TimelineIdGenerator;
import pt.haslab.horus.util.NamedThreadFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

public class TimelineEventAppenderProcessor implements AutoCloseable {
    private final static Logger logger = LogManager.getLogger(TimelineEventAppenderProcessor.class);
    private final BlockingQueue<Event> outgoingEvents;
    private final TimelineIdGenerator timelineIdGenerator;
    private final ExecutionGraph graph;
    private int flushPeriod;
    private int flushSizeThreshold;
    private ReentrantLock flushLock;
    private HashMap<String, SortedSet<Event>> timelineEvents;
    private HashMap<String, Event> lastTimelineEvents;
    private ScheduledExecutorService flushTaskScheduleExecutor;
    private ExecutorService timelineFlushersExecutor;
    private int timelineFlusherThreads;
    private ScheduledFuture<?> currentPeriodicFlushTask;
    private final MetricRegistry statsRegistry = SharedMetricRegistries.getOrCreate("stats");

    public TimelineEventAppenderProcessor(ExecutionGraph graph, BlockingQueue<Event> outgoingEvents, Configuration configuration) {
        this.flushPeriod = configuration.getInt("async.timelineAppender.flushPeriodThreshold", 10000);
        assert this.flushPeriod > 0;

        this.flushSizeThreshold = configuration.getInt("async.timelineAppender.flushSizeThreshold", 5000);
        assert this.flushSizeThreshold >= 0;

        this.timelineIdGenerator = graph.getTimelineIdGenerator();
        this.outgoingEvents = outgoingEvents;
        this.timelineEvents = new HashMap<>();
        this.lastTimelineEvents = new HashMap<>();
        this.flushTaskScheduleExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("appender-flusher-"));
        this.timelineFlusherThreads = configuration.getInt("async.timelineAppender.storeEventThreads", Runtime.getRuntime().availableProcessors());
        this.timelineFlushersExecutor = Executors.newFixedThreadPool(this.timelineFlusherThreads);
        this.graph = graph;
        this.flushLock = new ReentrantLock();

        this.statsRegistry.counter(MetricRegistry.name(TimelineEventAppenderProcessor.class, "timeline-appender-buffer-size"));
    }

    public void process(Event event) {
        try {
            this.flushLock.lock();
            this.addEventToTimeline(event);

            this.statsRegistry.counter(MetricRegistry.name(TimelineEventAppenderProcessor.class, "timeline-appender-buffer-size")).inc();

            if (this.flushSizeThreshold > 0 && this.timelineEvents.values().size() > this.flushSizeThreshold) {
                logger.info("Triggering flush due to size threshold.");
                this.flush();
            }
        } catch (OutOfOrderException e) {
            logger.info("Discarding event: "+ e.getMessage());
            this.statsRegistry.counter(MetricRegistry.name(TimelineEventAppenderProcessor.class, "out-of-order-events")).inc();
        } finally {
            this.flushLock.unlock();
        }

        if (this.currentPeriodicFlushTask == null) {
            this.schedulePeriodicFlush();
        }
    }

    private void schedulePeriodicFlush() {
        if (logger.isDebugEnabled())
            logger.debug("(Re-)Scheduling periodic flush task to run within " + this.flushPeriod + " milliseconds.");

        this.currentPeriodicFlushTask = this.flushTaskScheduleExecutor.scheduleAtFixedRate(this::flush, this.flushPeriod, this.flushPeriod, TimeUnit.MILLISECONDS);
    }

    public void flush() {
        try {
            this.flushLock.lock();

            int splitWork = (int) Math.ceil((double) this.timelineEvents.size() / this.timelineFlusherThreads);
            splitWork = splitWork > 0 ? splitWork : 1;

            List<Callable<StoreEventResult>> storeEventTaskCallables = new ArrayList<>();
            for (List<Map.Entry<String, SortedSet<Event>>> partitions : ListUtils.partition(new ArrayList<>(this.timelineEvents.entrySet()), splitWork)) {
                storeEventTaskCallables.add(() -> {

                    StoreEventResult result = new StoreEventResult();

                    // We now proceed by handling events for each process timeline.
                    for (Map.Entry<String, SortedSet<Event>> timelineEvents : partitions) {
                        List<Pair<Event, Event>> timelineRelationships = new ArrayList<>();
                        String timelineIdentifier = timelineEvents.getKey();
                        List<Event> timelineEventsList = new ArrayList<>(timelineEvents.getValue());

                        Event previousTimelineEvent = this.lastTimelineEvents.get(timelineIdentifier);

                        for (Event event : timelineEvents.getValue()) {
                            if (previousTimelineEvent != null) {
                                timelineRelationships.add(new ImmutablePair<>(previousTimelineEvent, event));
                            }

                            previousTimelineEvent = event;
                        }

                        result.setLastTimelineEvent(timelineIdentifier, previousTimelineEvent);

                        Timer.Context insertEventMetricTimer = this.statsRegistry.timer(MetricRegistry.name(TimelineEventAppenderProcessor.class, "insert-event")).time();
                        this.graph.storeEvents(timelineEventsList);
                        insertEventMetricTimer.stop();

                        Timer.Context insertRelationshipsMetricTimer = this.statsRegistry.timer(MetricRegistry.name(TimelineEventAppenderProcessor.class, "insert-relationships")).time();
                        timelineRelationships.sort(Comparator.comparing(causalPair -> causalPair.getKey().getId()));
                        this.graph.storeRelationships(timelineRelationships);
                        insertRelationshipsMetricTimer.stop();

                        int inserted = 0;
                        while (inserted < timelineEventsList.size()) {
                            try {
                                this.outgoingEvents.put(timelineEventsList.get(inserted));
                                inserted++;
                            } catch (InterruptedException e) {
                                logger.info("Interrupted while sending events to next processor.");
                            }
                        }
                    }

                    return result;
                });
            }

            List<Future<StoreEventResult>> timelineFlushResults = this.timelineFlushersExecutor.invokeAll(storeEventTaskCallables);

            for (Future<StoreEventResult> timelineFlushResult : timelineFlushResults) {
                try {
                    StoreEventResult result = timelineFlushResult.get();

                    this.lastTimelineEvents.putAll(result.getLastTimelineEventPerTimeline());
                } catch(InterruptedException e) {
                    logger.warn("Interrupted while waiting for flusher results.");
                }
            }

            this.timelineEvents.clear();
            this.statsRegistry.counter(MetricRegistry.name(TimelineEventAppenderProcessor.class, "timeline-appender-buffer-size")).dec(timelineEvents.size());
        } catch (Exception e) {
            logger.error("Timeline Appender Flusher got an exception. ", e);
        } finally {
            if (this.flushLock.isHeldByCurrentThread()) {
                this.flushLock.unlock();
            }
        }
    }


    private void addEventToTimeline(Event event) throws OutOfOrderException {
        SortedSet<Event> timeline = this.getTimelineEventSet(event);
        Event lastTimelineEvent = this.lastTimelineEvents.get(this.timelineIdGenerator.getTimelineId(event));

        if (lastTimelineEvent != null && lastTimelineEvent.getKernelTime() >= event.getKernelTime()) {
            throw new OutOfOrderException(event, this.timelineIdGenerator.getTimelineId(event));
        }

        timeline.add(event);
    }

    private SortedSet<Event> getTimelineEventSet(Event event) {
        String timelineId = this.timelineIdGenerator.getTimelineId(event);

        SortedSet<Event> events;
        if ((events = this.timelineEvents.get(timelineId)) == null) {
            this.timelineEvents.put(timelineId, events = new TreeSet<>(new KernelTimeComparator()));
        }

        return events;
    }

    @Override
    public void close() throws IOException {
        this.flushTaskScheduleExecutor.shutdown();

        while (!this.flushTaskScheduleExecutor.isTerminated()) {
            logger.info("Waiting for periodic flush to terminate...");
            try {
                this.timelineFlushersExecutor.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
            }
        }

        // Perform the last flush, if needed, before closing the processor.
        if (this.timelineEvents.values().size() > 0) {
            logger.info("Flushing last events before closing...");
            this.flush();
        }
    }

    private class StoreEventResult {
        private Map<String, Event> lastTimelineEvent;

        public StoreEventResult() {
            this.lastTimelineEvent = new HashMap<>();
        }

        public void setLastTimelineEvent(String timeline, Event event) {
            this.lastTimelineEvent.put(timeline, event);
        }

        public Map<String, Event> getLastTimelineEventPerTimeline() {
            return this.lastTimelineEvent;
        }
    }
}
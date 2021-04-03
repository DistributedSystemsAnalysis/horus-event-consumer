package pt.haslab.horus.processing.processors;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.haslab.horus.Config;
import pt.haslab.horus.events.*;
import pt.haslab.horus.graph.ExecutionGraph;
import pt.haslab.horus.util.NamedThreadFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

public class EventHappensBeforeRelationProcessor implements Runnable, AutoCloseable {
    private final static Logger logger = LogManager.getLogger(EventHappensBeforeRelationProcessor.class);
    private final BlockingQueue<Event> incomingEvents;
    private final ReentrantLock flushLock;
    private final int flushPeriod;
    private final int flushSizeThreshold;
    private final int processingBatchSize;
    private List<Pair<Event, Event>> establishedRelationships;
    private ProcessEventsHappensBeforeEstablisher processEventsEstablisher;
    private SocketEventsHappensBeforeEstablisher socketEventsEstablisher;
    private final ExecutionGraph graph;
    private ScheduledExecutorService flushTaskScheduleExecutor;
    private ScheduledFuture<?> currentPeriodicFlushTask;
    private boolean closed = false;
    private final MetricRegistry statsRegistry = SharedMetricRegistries.getOrCreate("stats");
    private Meter processedEventsMeter = null;
    private Meter processedRelationshipsMeter = null;

    public EventHappensBeforeRelationProcessor(ExecutionGraph graph, BlockingQueue<Event> incomingEvents, Configuration configuration) {
        this.flushPeriod = configuration.getInt("async.relationshipEstablisher.flushPeriodThreshold", 10000);
        assert this.flushPeriod > 0;

        this.flushSizeThreshold = configuration.getInt("async.relationshipEstablisher.flushSizeThreshold", 5000);
        assert this.flushSizeThreshold >= 0;

        this.processingBatchSize= configuration.getInt("async.relationshipEstablisher.processingBatch", 1000);

        this.incomingEvents = incomingEvents;
        this.establishedRelationships = new ArrayList<>();
        this.processEventsEstablisher = new ProcessEventsHappensBeforeEstablisher();
        this.socketEventsEstablisher = new SocketEventsHappensBeforeEstablisher();
        this.flushTaskScheduleExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("hb-flusher-"));
        this.graph = graph;
        this.flushLock = new ReentrantLock();

        this.statsRegistry.counter(MetricRegistry.name(EventHappensBeforeRelationProcessor.class, "hb-establisher-buffer-size"));
        processedEventsMeter = this.statsRegistry.meter("processed-events");
        processedRelationshipsMeter = this.statsRegistry.meter("processed-relationships");
    }

    @Override
    public void run() {
        try {
            int batchSize = this.processingBatchSize;
            List<Event> eventsList = new ArrayList<>(batchSize);

            if (logger.isDebugEnabled()) {
                logger.debug("Happens-before processor started.");
            }

            boolean stop = false;
            int triesRemainingBeforeClosing = 3;
            int readEvents = 0;
            while (triesRemainingBeforeClosing > 0 || readEvents > 0) {
                readEvents = this.incomingEvents.drainTo(eventsList, batchSize);

                if (logger.isDebugEnabled()) {
                    logger.debug("Read " + readEvents + " events from buffer.");
                }

                if (readEvents == 0) {
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException e) {
                        logger.info("DrainTo loop interrupted.");
                    }

                    if (this.closed) {
                        triesRemainingBeforeClosing--;
                    }

                    continue;
                }

                triesRemainingBeforeClosing = 3;

                this.flushLock.lock();
                eventsList.forEach(this::process);
                eventsList.clear();

                if (this.flushSizeThreshold > 0 && this.establishedRelationships.size() > this.flushSizeThreshold) {
                    logger.info("Triggering flush due to size threshold.");
                    this.flush();
                }
                this.flushLock.unlock();

                if (this.currentPeriodicFlushTask == null) {
                    this.schedulePeriodicFlush();
                }
            }

            this.shutdown();
        } catch (Exception e) {
            logger.error("Happens Before Processor got an exception. ", e);
        }
    }

    public void flush() {
        this.flushLock.lock();
        try {
            ArrayList<Pair<Event, Event>> relationships = new ArrayList<>(this.establishedRelationships);
            this.establishedRelationships.clear();
            this.statsRegistry.counter(MetricRegistry.name(EventHappensBeforeRelationProcessor.class, "hb-establisher-buffer-size")).dec(relationships.size());
            this.flushLock.unlock();

            if (relationships.size() == 0) {
                return;
            }

            // Store relationships in graph database.
            relationships.sort(Comparator.comparing(causalPair -> causalPair.getKey().getId()));
            logger.info("Flushing " + relationships.size() + " relationships...");
            Timer.Context insertRelationshipsMetricTimer = this.statsRegistry.timer(MetricRegistry.name(TimelineEventAppenderProcessor.class, "insert-relationships")).time();
            this.graph.storeRelationships(relationships);
            insertRelationshipsMetricTimer.stop();
            this.processedEventsMeter.mark(relationships.size() * 2);
            this.processedRelationshipsMeter.mark(relationships.size());
        } catch (Exception e) {
            logger.error("Happens Before Flusher got an exception: " + e.getMessage());
            logger.error(e.getStackTrace());
        } finally {
            if (this.flushLock.isHeldByCurrentThread()) {
                this.flushLock.unlock();
            }
        }
    }

    private void schedulePeriodicFlush() {
        if (logger.isDebugEnabled())
            logger.debug("(Re-)Scheduling periodic flush task to run within " + this.flushPeriod + " milliseconds.");

        this.currentPeriodicFlushTask = this.flushTaskScheduleExecutor.scheduleAtFixedRate(this::flush, this.flushPeriod, this.flushPeriod, TimeUnit.MILLISECONDS);
    }

    private void process(Event event) {
        Collection<Pair<Event, Event>> establishedRelationships = this.findRelationshipsForEvent(event);

        if (establishedRelationships == null || establishedRelationships.size() == 0) {
            return;
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Found " + establishedRelationships.size() + " relationships between events.");
        }

        this.establishedRelationships.addAll(establishedRelationships);
        this.statsRegistry.counter(MetricRegistry.name(EventHappensBeforeRelationProcessor.class, "hb-establisher-buffer-size")).inc(establishedRelationships.size());
    }

    private Collection<Pair<Event, Event>> findRelationshipsForEvent(Event event) {
        if (event instanceof ProcessCreate || event instanceof ProcessStart || event instanceof ProcessJoin || event instanceof ProcessEnd) {
            return this.processEventsEstablisher.establish(event);
        }

        if (event instanceof SocketConnect || event instanceof SocketAccept || event instanceof SocketSend || event instanceof SocketReceive) {
            return this.socketEventsEstablisher.establish(event);
        }

        return null;
    }

    private void shutdown() {
        logger.info("Starting to shutdown happens before establisher...");
        this.flushTaskScheduleExecutor.shutdown();

        while (!this.flushTaskScheduleExecutor.isTerminated()) {
            logger.info("Waiting for periodic flush to terminate...");
            try {
                this.flushTaskScheduleExecutor.awaitTermination(60, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
            }
        }

        // Perform the last flush, if needed, before closing the processor.
        if (this.establishedRelationships.size() > 0) {
            logger.info("Flushing last relationships before closing...");
            this.flush();
        }
    }


    @Override
    public void close() throws IOException {
        this.closed = true;
    }
}

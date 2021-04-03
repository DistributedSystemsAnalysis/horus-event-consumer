package pt.haslab.horus.processing.processors;

import com.codahale.metrics.SharedMetricRegistries;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pt.haslab.horus.Config;
import pt.haslab.horus.events.Event;
import pt.haslab.horus.events.SocketConnect;
import pt.haslab.horus.graph.datastores.MemoryExecutionGraph;
import pt.haslab.horus.processing.EventProcessorPipeline;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventProcessorPipelineTest {

    private EventProcessorPipeline pipeline;
    private MemoryExecutionGraph graph;

    @BeforeEach
    void setUp() {
        SharedMetricRegistries.clear();
        this.graph = new MemoryExecutionGraph();
        this.pipeline = new EventProcessorPipeline(this.graph, Config.getInstance().getConfig());
    }

    @Test
    public void storesEventsSubmittedToPipeline() throws Exception {
        List<Event> eventsToProcess = new ArrayList<>();
        eventsToProcess.addAll(EventUtil.getSendEvents(10, 10));
        eventsToProcess.addAll(EventUtil.getReceiveEvents(5, 10, 5));

        eventsToProcess.forEach(this.pipeline::process);

        this.pipeline.close();

        Collection<Event> events = this.graph.getEvents();
        Collection<Pair<Event, Event>> relationships = this.graph.getRelationships();

        assertEquals(eventsToProcess.size(), events.size());

        // Assert expected events.
        Map<String, List<Event>> expectedTimelineEvents = eventsToProcess.stream().collect(Collectors.groupingBy(Event::getHost));
        for (Event event : events) {
            assertTrue(expectedTimelineEvents.get(event.getHost()).contains(event));
        }

        List<Pair<Event, Event>> expectedRelationships = new ArrayList<Pair<Event, Event>>() {{
            add(new ImmutablePair<>(eventsToProcess.get(0), eventsToProcess.get(1)));
            add(new ImmutablePair<>(eventsToProcess.get(2), eventsToProcess.get(3)));
            add(new ImmutablePair<>(eventsToProcess.get(3), eventsToProcess.get(4)));
            add(new ImmutablePair<>(eventsToProcess.get(0), eventsToProcess.get(2)));
            add(new ImmutablePair<>(eventsToProcess.get(0), eventsToProcess.get(3)));
            add(new ImmutablePair<>(eventsToProcess.get(1), eventsToProcess.get(3)));
            add(new ImmutablePair<>(eventsToProcess.get(1), eventsToProcess.get(4)));
        }};

        // Assert expected relationships.
        assertEquals(expectedRelationships.size(), relationships.size());
        for (Pair<Event, Event> relationship : expectedRelationships) {
            assertTrue(relationships.contains(relationship), "Could not find relationship between " + relationship.getLeft().getId() + " and " + relationship.getRight().getId() + " events.");
        }
    }

    @Test
    public void outOfOrderEventsAreDiscarded() throws Exception {
        List<Event> eventsToProcess = new ArrayList<>();
        eventsToProcess.addAll(EventUtil.getSendEvents(10, 10));
        eventsToProcess.addAll(EventUtil.getReceiveEvents(5, 10, 5));

        eventsToProcess.forEach(this.pipeline::process);

        // Wait for flush to be triggered.
        Thread.sleep(10000);

        // Add an out of order event.
        this.pipeline.process(new SocketConnect() {{
            setId("connect");
            setUserTime(1);
            setKernelTime(1);
            setPid(9);
            setTid(10);
            setComm("comm");
            setHost("host");
            setSocketId("socket-id");
            setSocketFamily(10);
            setSocketFrom("0.0.0.0");
            setSourcePort(10);
            setSocketTo("0.0.0.0");
            setDestinationPort(10);
        }});

        this.pipeline.close();

        Collection<Event> events = this.graph.getEvents();
        Collection<Pair<Event, Event>> relationships = this.graph.getRelationships();

        assertEquals(eventsToProcess.size(), events.size());

        // Assert expected events.
        Iterator<Event> expectedEventsIterator = eventsToProcess.iterator();
        for (Event event : events) {
            assertEquals(expectedEventsIterator.next().getId(), event.getId());
        }

        List<Pair<Event, Event>> expectedRelationships = new ArrayList<Pair<Event, Event>>() {{
            add(new ImmutablePair<>(eventsToProcess.get(0), eventsToProcess.get(1)));
            add(new ImmutablePair<>(eventsToProcess.get(2), eventsToProcess.get(3)));
            add(new ImmutablePair<>(eventsToProcess.get(3), eventsToProcess.get(4)));
            add(new ImmutablePair<>(eventsToProcess.get(0), eventsToProcess.get(2)));
            add(new ImmutablePair<>(eventsToProcess.get(0), eventsToProcess.get(3)));
            add(new ImmutablePair<>(eventsToProcess.get(1), eventsToProcess.get(3)));
            add(new ImmutablePair<>(eventsToProcess.get(1), eventsToProcess.get(4)));
        }};

        // Assert expected relationships.
        assertEquals(expectedRelationships.size(), relationships.size());
        for (Pair<Event, Event> relationship : expectedRelationships) {
            assertTrue(relationships.contains(relationship), "Could not find relationship between " + relationship.getLeft().getId() + " and " + relationship.getRight().getId() + " events.");
        }
    }

}
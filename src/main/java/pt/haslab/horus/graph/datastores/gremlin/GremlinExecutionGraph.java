package pt.haslab.horus.graph.datastores.gremlin;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Triplet;
import pt.haslab.horus.events.*;
import pt.haslab.horus.graph.AbstractExecutionGraph;
import pt.haslab.horus.graph.timeline.TimelineIdGeneratorFactory;

import java.util.*;

public class GremlinExecutionGraph extends AbstractExecutionGraph {
    private final static Logger logger = LogManager.getLogger(GremlinExecutionGraph.class);

    public static String HAPPENS_BEFORE_LABEL = "happens_before";

    public static String EVENT_ID = "event_id";

    private boolean useTransactions = true;

    private HashMap<String, Long> lastSeenKernelTimestamps;

    Graph graph;

    GraphTraversalSource graphTraversal;

    public GremlinExecutionGraph(Graph graph) {
        logger.info(graph.features());
        this.graph = graph;
        this.graphTraversal = graph.traversal();
        this.useTransactions = graph.features().graph().supportsTransactions();
        this.lastSeenKernelTimestamps = new HashMap<>();
        this.timelineIdGenerator = TimelineIdGeneratorFactory.get();
    }

    @Override
    public void addEvent(ProcessCreate event) {
        GraphTraversal<Vertex, Vertex> createEventTraversal = this.graphTraversal.addV(EventType.CREATE.toString())
                .property(GremlinExecutionGraph.EVENT_ID,
                        event.getId())
                .property("userTime", event.getUserTime())
                .property("kernelTime", event.getKernelTime())
                .property("pid", event.getPid())
                .property("tid", event.getTid())
                .property("comm", event.getComm())
                .property("host", event.getHost())
                .property("threadId", this.timelineIdGenerator.getTimelineId(event))
                .property("childPid", event.getChildPid());

        this.addPropertiesFromJson(createEventTraversal, event.getExtraData());

        Vertex createEvent = createEventTraversal.next();
        this.appendThreadEvent(this.timelineIdGenerator.getTimelineId(event), createEvent, event);

        try {
            Vertex startEvent = this.graphTraversal.V()
                    .hasLabel(EventType.START.toString())
                    .has("tid", event.getChildPid())
                    .has("host", event.getHost())
                    .has("kernelTime", P.gte(event.getKernelTime()))
                    .where(__.not(__.in(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL).hasLabel(
                            EventType.CREATE.toString())))
                    .next();

            this.addEdge(createEvent, startEvent);
        } catch (NoSuchElementException e) {
            if (logger.isDebugEnabled())
                logger.debug("There's no candidate to create a causal relationship with event [" + event.getId()
                        + "].");
        }

        if (this.useTransactions)
            this.graphTraversal.tx().commit();
    }

    @Override
    public void addEvent(ProcessStart event) {
        GraphTraversal<Vertex, Vertex> startEventTraversal = this.graphTraversal.addV(EventType.START.toString())
                .property(GremlinExecutionGraph.EVENT_ID,
                        event.getId())
                .property("userTime", event.getUserTime())
                .property("kernelTime", event.getKernelTime())
                .property("pid", event.getPid())
                .property("tid", event.getTid())
                .property("comm", event.getComm())
                .property("host", event.getHost())
                .property("threadId", this.timelineIdGenerator.getTimelineId(event));

        this.addPropertiesFromJson(startEventTraversal, event.getExtraData());

        Vertex startEvent = startEventTraversal.next();
        this.appendThreadEvent(this.timelineIdGenerator.getTimelineId(event), startEvent, event);

        try {
            Vertex createEvent = this.graphTraversal.V()
                    .hasLabel(EventType.CREATE.toString())
                    .has("childPid", event.getTid())
                    .has("host", event.getHost())
                    .has("kernelTime", P.lte(event.getKernelTime()))
                    .where(__.not(__.out(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL).hasLabel(
                            EventType.START.toString())))
                    .next();
            this.addEdge(createEvent, startEvent);
        } catch (NoSuchElementException e) {
            if (logger.isDebugEnabled())
                logger.debug("There's no candidate to create a causal relationship with event [" + event.getId()
                        + "].");
        }

        if (this.useTransactions)
            this.graphTraversal.tx().commit();
    }

    @Override
    public void addEvent(ProcessEnd event) {
        GraphTraversal<Vertex, Vertex> endEventTraversal = this.graphTraversal.addV(EventType.END.toString())
                .property(GremlinExecutionGraph.EVENT_ID, event.getId())
                .property("userTime", event.getUserTime())
                .property("kernelTime", event.getKernelTime())
                .property("pid", event.getPid())
                .property("tid", event.getTid())
                .property("comm", event.getComm())
                .property("host", event.getHost())
                .property("threadId", this.timelineIdGenerator.getTimelineId(event));

        this.addPropertiesFromJson(endEventTraversal, event.getExtraData());

        Vertex endEvent = endEventTraversal.next();
        this.appendThreadEvent(this.timelineIdGenerator.getTimelineId(event), endEvent, event);

        try {
            Vertex joinEvent = this.graphTraversal.V()
                    .hasLabel(EventType.JOIN.toString())
                    .has("childPid", event.getTid())
                    .has("host", event.getHost())
                    .has("kernelTime", P.gte(event.getKernelTime()))
                    .where(__.not(__.in(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL).hasLabel(
                            EventType.END.toString())))
                    .next();
            this.addEdge(endEvent, joinEvent);
        } catch (NoSuchElementException e) {
            if (logger.isDebugEnabled())
                logger.debug("There's no candidate to create a causal relationship with event [" + event.getId()
                        + "].");
        }

        if (this.useTransactions)
            this.graphTraversal.tx().commit();
    }

    @Override
    public void addEvent(ProcessJoin event) {
        GraphTraversal<Vertex, Vertex> joinEventTraversal = this.graphTraversal.addV(EventType.JOIN.toString())
                .property(GremlinExecutionGraph.EVENT_ID,
                        event.getId())
                .property("userTime", event.getUserTime())
                .property("kernelTime", event.getKernelTime())
                .property("pid", event.getPid())
                .property("tid", event.getTid())
                .property("comm", event.getComm())
                .property("host", event.getHost())
                .property("threadId", this.timelineIdGenerator.getTimelineId(event))
                .property("childPid", event.getChildPid());

        this.addPropertiesFromJson(joinEventTraversal, event.getExtraData());

        Vertex joinEvent = joinEventTraversal.next();
        this.appendThreadEvent(this.timelineIdGenerator.getTimelineId(event), joinEvent, event);

        try {
            Vertex endEvent = this.graphTraversal.V()
                    .hasLabel(EventType.END.toString())
                    .has("tid", event.getChildPid())
                    .has("host", event.getHost())
                    .has("kernelTime", P.lte(event.getKernelTime()))
                    .where(__.not(__.out(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL).hasLabel(
                            EventType.JOIN.toString())))
                    .next();
            this.addEdge(endEvent, joinEvent);
        } catch (NoSuchElementException e) {
            if (logger.isDebugEnabled())
                logger.debug("There's no candidate to create a causal relationship with event [" + event.getId()
                        + "].");
        }

        if (this.useTransactions)
            this.graphTraversal.tx().commit();
    }

    @Override
    public void addEvent(SocketAccept event) {
        GraphTraversal<Vertex, Vertex> acceptEventTraversal = this.graphTraversal.addV(EventType.ACCEPT.toString())
                .property(GremlinExecutionGraph.EVENT_ID,
                        event.getId())
                .property("userTime", event.getUserTime())
                .property("kernelTime", event.getKernelTime())
                .property("pid", event.getPid())
                .property("tid", event.getTid())
                .property("comm", event.getComm())
                .property("host", event.getHost())
                .property("threadId", this.timelineIdGenerator.getTimelineId(event))
                .property("socketId", event.getSocketId())
                .property("socketFamily",
                        event.getSocketFamily())
                .property("socketFrom", event.getSocketFrom())
                .property("socketFromPort",
                        event.getSourcePort())
                .property("socketTo", event.getSocketTo())
                .property("socketToPort",
                        event.getSourcePort());

        this.addPropertiesFromJson(acceptEventTraversal, event.getExtraData());

        Vertex acceptEvent = acceptEventTraversal.next();
        this.appendThreadEvent(this.timelineIdGenerator.getTimelineId(event), acceptEvent, event);

        try {
            Vertex connectEvent = this.graphTraversal.V()
                    .hasLabel(EventType.CONNECT.toString())
                    .has("socketId", event.getSocketId())
                    .has("socketFamily", event.getSocketFamily())
                    .where(__.not(__.out(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL).hasLabel(
                            EventType.ACCEPT.toString())))
                    .next();
            this.addEdge(connectEvent, acceptEvent);
        } catch (NoSuchElementException e) {
            if (logger.isDebugEnabled())
                logger.debug("There's no candidate to create a causal relationship with event [" + event.getId()
                        + "].");
        }

        if (this.useTransactions)
            this.graphTraversal.tx().commit();
    }

    @Override
    public void addEvent(SocketConnect event) {
        GraphTraversal<Vertex, Vertex> connectEventTraversal = this.graphTraversal.addV(EventType.CONNECT.toString())
                .property(GremlinExecutionGraph.EVENT_ID,
                        event.getId())
                .property("userTime", event.getUserTime())
                .property("kernelTime",
                        event.getKernelTime())
                .property("pid", event.getPid())
                .property("tid", event.getTid())
                .property("comm", event.getComm())
                .property("host", event.getHost())
                .property("threadId", this.timelineIdGenerator.getTimelineId(event))
                .property("socketId", event.getSocketId())
                .property("socketFamily",
                        event.getSocketFamily())
                .property("socketFrom",
                        event.getSocketFrom())
                .property("socketFromPort",
                        event.getSourcePort())
                .property("socketTo", event.getSocketTo())
                .property("socketToPort",
                        event.getSourcePort());

        this.addPropertiesFromJson(connectEventTraversal, event.getExtraData());

        Vertex connectEvent = connectEventTraversal.next();
        this.appendThreadEvent(this.timelineIdGenerator.getTimelineId(event), connectEvent, event);

        try {
            Vertex acceptEvent = this.graphTraversal.V()
                    .hasLabel(EventType.ACCEPT.toString())
                    .has("socketId", event.getSocketId())
                    .has("socketFamily", event.getSocketFamily())
                    .where(__.not(__.in(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL).hasLabel(
                            EventType.CONNECT.toString())))
                    .next();
            this.addEdge(connectEvent, acceptEvent);
        } catch (NoSuchElementException e) {
            if (logger.isDebugEnabled())
                logger.debug("There's no candidate to create a causal relationship with event [" + event.getId()
                        + "].");
        }

        if (this.useTransactions)
            this.graphTraversal.tx().commit();
    }

    @Override
    public void addEvent(SocketSend event) {
        GraphTraversal<Vertex, Vertex> sendEventTraversal = this.graphTraversal.addV(EventType.SND.toString())
                .property(GremlinExecutionGraph.EVENT_ID,
                        event.getId())
                .property("userTime", event.getUserTime())
                .property("kernelTime", event.getKernelTime())
                .property("pid", event.getPid())
                .property("tid", event.getTid())
                .property("comm", event.getComm())
                .property("host", event.getHost())
                .property("threadId", this.timelineIdGenerator.getTimelineId(event))
                .property("socketId", event.getSocketId())
                .property("socketFamily",
                        event.getSocketFamily())
                .property("socketFrom", event.getSocketFrom())
                .property("socketFromPort",
                        event.getSourcePort())
                .property("socketTo", event.getSocketTo())
                .property("socketToPort", event.getSourcePort())
                .property("size", event.getSize());

        this.addPropertiesFromJson(sendEventTraversal, event.getExtraData());

        Vertex sendEvent = sendEventTraversal.next();
        this.appendThreadEvent(this.timelineIdGenerator.getTimelineId(event), sendEvent, event);

        boolean continueSearch;
        long fulfillSize = 0;
        boolean fulfilled = false;
        List<Triplet<Vertex, Long, Long>> incompleteRcvs = new ArrayList<>();
        long lastSeenKernelTime = this.getLastCompleteRcvKernelTime(event.getHost(), event.getSocketId());

        do {
            List<Map<String, Object>> latestRcvQueryResult = this.graphTraversal.V()
                    .has("socketId", event.getSocketId())
                    .has("socketFamily", event.getSocketFamily())
                    .hasLabel(EventType.RCV.toString())
                    .has("kernelTime", P.gt(lastSeenKernelTime))
                    .order().by("kernelTime", Order.incr)
                    .project("event", "size", "kernelTime",
                            "fullfillSize")
                    .by()
                    .by("size")
                    .by("kernelTime")
                    .by(__.inE(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL)
                            .where(__.outV().hasLabel(
                                    EventType.SND.toString()))
                            .values("size")
                            .sum()
                    )
                    .next(10);

            continueSearch = latestRcvQueryResult.size() > 0;

            Iterator<Map<String, Object>> latestRcvs = latestRcvQueryResult.iterator();
            while (latestRcvs.hasNext() && continueSearch) {
                Map<String, Object> rcvData = latestRcvs.next();

                Vertex rcvVertex = (Vertex) rcvData.get("event");
                long rcvSize = ((Number) rcvData.get("size")).longValue();
                long rcvFulfillSize = ((Number) rcvData.get("fullfillSize")).longValue();
                long remainingSize = rcvSize - rcvFulfillSize;

                // Update last seen kernel time in order to skip past events.
                lastSeenKernelTime = ((Number) rcvData.get("kernelTime")).longValue();

                if (remainingSize > 0) {
                    long sizeToFulfill = Math.min(remainingSize, event.getSize() - fulfillSize);
                    incompleteRcvs.add(new Triplet<>(rcvVertex, sizeToFulfill, remainingSize));

                    // We only continue searching if we need more events to met the size of the new event.
                    fulfillSize += sizeToFulfill;
                    continueSearch = fulfillSize < event.getSize();
                    fulfilled = fulfillSize == event.getSize();

                } else {
                    // Update the last complete RCV timestamp, so we can skip complete events on next SND-matching operation.
                    this.updateLastCompleteRcvKernelTime(event.getHost(), event.getSocketId(), lastSeenKernelTime);
                }
            }
        }
        while (continueSearch);

        for (Triplet<Vertex, Long, Long> rcv : incompleteRcvs) {
            this.addEdge(sendEvent, rcv.getValue0(), "size", rcv.getValue1());

            if (rcv.getValue2().equals(rcv.getValue1())) {
                this.updateLastCompleteRcvKernelTime(event.getHost(), event.getSocketId(), lastSeenKernelTime);
            }
        }

        if (fulfilled)
            this.updateLastCompleteSndKernelTime(event.getHost(), event.getSocketId(), event.getKernelTime());

        if (this.useTransactions)
            this.graphTraversal.tx().commit();
    }


    public long getLastCompleteSndKernelTime(String host, String socketId) {
        String id = host + ";" + socketId + ";" + "SND";

        return this.lastSeenKernelTimestamps.getOrDefault(id, 0L);
    }

    public long getLastCompleteRcvKernelTime(String host, String socketId) {
        String id = host + ";" + socketId + ";" + "RCV";

        return this.lastSeenKernelTimestamps.getOrDefault(id, 0L);
    }

    private void updateLastCompleteSndKernelTime(String host, String socketId, long timestamp) {
        String id = host + ";" + socketId + ";" + "SND";

        this.lastSeenKernelTimestamps.put(id, timestamp);
    }

    private void updateLastCompleteRcvKernelTime(String host, String socketId, long timestamp) {
        String id = host + ";" + socketId + ";" + "RCV";

        this.lastSeenKernelTimestamps.put(id, timestamp);
    }

    @Override
    public void addEvent(SocketReceive event) {
        GraphTraversal<Vertex, Vertex> receiveEventTraversal = this.graphTraversal.addV(EventType.RCV.toString())
                .property(GremlinExecutionGraph.EVENT_ID,
                        event.getId())
                .property("userTime", event.getUserTime())
                .property("kernelTime",
                        event.getKernelTime())
                .property("pid", event.getPid())
                .property("tid", event.getTid())
                .property("comm", event.getComm())
                .property("host", event.getHost())
                .property("threadId", this.timelineIdGenerator.getTimelineId(event))
                .property("socketId", event.getSocketId())
                .property("socketFamily",
                        event.getSocketFamily())
                .property("socketFrom",
                        event.getSocketFrom())
                .property("socketFromPort",
                        event.getSourcePort())
                .property("socketTo", event.getSocketTo())
                .property("socketToPort",
                        event.getSourcePort())
                .property("size", event.getSize());

        this.addPropertiesFromJson(receiveEventTraversal, event.getExtraData());

        Vertex receiveEvent = receiveEventTraversal.next();
        this.appendThreadEvent(this.timelineIdGenerator.getTimelineId(event), receiveEvent, event);

        boolean continueSearch;
        long fulfillSize = 0;
        boolean fulfilled = false;
        List<Triplet<Vertex, Long, Long>> incompleteSnds = new ArrayList<>();
        long lastSeenKernelTime = this.getLastCompleteSndKernelTime(event.getHost(), event.getSocketId());

        do {
            List<Map<String, Object>> latestSndQueryResult = this.graphTraversal.V()
                    .has("socketId", event.getSocketId())
                    .has("socketFamily", event.getSocketFamily())
                    .hasLabel(EventType.SND.toString())
                    .has("kernelTime", P.gt(lastSeenKernelTime))
                    .order().by("kernelTime", Order.incr)
                    .project("event", "size", "kernelTime",
                            "fullfillSize")
                    .by()
                    .by("size")
                    .by("kernelTime")
                    .by(__.outE(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL)
                            .where(__.inV().hasLabel(
                                    EventType.RCV.toString()))
                            .values("size")
                            .sum()
                    )
                    .next(10);

            continueSearch = latestSndQueryResult.size() > 0;

            Iterator<Map<String, Object>> latestSnds = latestSndQueryResult.iterator();
            while (latestSnds.hasNext() && continueSearch) {
                Map<String, Object> sndData = latestSnds.next();

                Vertex sndVertex = (Vertex) sndData.get("event");
                long sndSize = ((Number) sndData.get("size")).longValue();
                long sndFulfillSize = ((Number) sndData.get("fullfillSize")).longValue();
                long remainingSize = sndSize - sndFulfillSize;

                // Update last seen kernel time in order to skip past events.
                lastSeenKernelTime = ((Number) sndData.get("kernelTime")).longValue();

                if (remainingSize > 0) {
                    long sizeToFulfill = Math.min(remainingSize, event.getSize() - fulfillSize);
                    incompleteSnds.add(new Triplet<>(sndVertex, sizeToFulfill, remainingSize));

                    // We only continue searching if we need more events to met the size of the new event.
                    fulfillSize += sizeToFulfill;
                    continueSearch = fulfillSize < event.getSize();
                    fulfilled = fulfillSize == event.getSize();

                } else {
                    // Update the last complete SND timestamp, so we can skip complete events on next RCV-matching operation.
                    this.updateLastCompleteSndKernelTime(event.getHost(), event.getSocketId(), lastSeenKernelTime);
                }
            }
        }
        while (continueSearch);

        for (Triplet<Vertex, Long, Long> snd : incompleteSnds) {
            this.addEdge(snd.getValue0(), receiveEvent, "size", snd.getValue1());

            if (snd.getValue2().equals(snd.getValue1())) {
                this.updateLastCompleteSndKernelTime(event.getHost(), event.getSocketId(), lastSeenKernelTime);
            }
        }

        if (fulfilled)
            this.updateLastCompleteRcvKernelTime(event.getHost(), event.getSocketId(), event.getKernelTime());

        if (this.useTransactions)
            this.graphTraversal.tx().commit();
    }

    @Override
    public void addEvent(Log event) {
        GraphTraversal<Vertex, Vertex> logEventTraversal = this.graphTraversal.addV(EventType.LOG.toString())
                .property(GremlinExecutionGraph.EVENT_ID,
                        event.getId())
                .property("userTime", event.getUserTime())
                .property("kernelTime", event.getKernelTime())
                .property("pid", event.getPid())
                .property("tid", event.getTid())
                .property("comm", event.getComm())
                .property("host", event.getHost())
                .property("threadId", this.timelineIdGenerator.getTimelineId(event));

        this.addPropertiesFromJson(logEventTraversal, event.getExtraData());

        Vertex createEvent = logEventTraversal.next();
        this.appendThreadEvent(this.timelineIdGenerator.getTimelineId(event), createEvent, event);

        if (this.useTransactions)
            this.graphTraversal.tx().commit();
    }

    @Override
    public void addEvent(FSync event) {
        GraphTraversal<Vertex, Vertex> fsyncEventTraversal = this.graphTraversal.addV(EventType.FSYNC.toString())
                .property(GremlinExecutionGraph.EVENT_ID,
                        event.getId())
                .property("userTime", event.getUserTime())
                .property("kernelTime", event.getKernelTime())
                .property("pid", event.getPid())
                .property("tid", event.getTid())
                .property("comm", event.getComm())
                .property("host", event.getHost())
                .property("threadId", this.timelineIdGenerator.getTimelineId(event));

        this.addPropertiesFromJson(fsyncEventTraversal, event.getExtraData());

        Vertex createEvent = fsyncEventTraversal.next();
        this.appendThreadEvent(this.timelineIdGenerator.getTimelineId(event), createEvent, event);

        if (this.useTransactions)
            this.graphTraversal.tx().commit();
    }

    @Override
    public void addEvent(Event event) {

    }

    @Override
    public void storeEvents(List<Event> events) {

    }

    @Override
    public void storeRelationships(Collection<Pair<Event, Event>> relationships) {

    }

    public void addEvents(Collection<Event> events) {
        throw new UnsupportedOperationException();
    }

    public void addCausalRelationships(Collection<Pair<Event, Event>> causalRelationships) {
        throw new UnsupportedOperationException();
    }

    public void clearGraph() {
        GraphTraversal<Vertex, Vertex> deleteVertices = this.graphTraversal.V().drop();
        GraphTraversal<Edge, Edge> deleteEdges = this.graphTraversal.E().drop();

        if (deleteVertices.hasNext())
            deleteVertices.next();
        if (deleteEdges.hasNext())
            deleteEdges.next();

        this.lastSeenKernelTimestamps.clear();

        if (this.useTransactions)
            this.graphTraversal.tx().commit();
    }

    public long countEvents() {
        GraphTraversal<Vertex, Long> countVertices = this.graphTraversal.V().count();

        if (countVertices.hasNext())
            return countVertices.next();

        return 0;
    }

    public Graph getGraph() {
        return this.graph;
    }

    public GraphTraversalSource getTraversal() {
        return this.graphTraversal;
    }

    private void addPropertiesFromJson(GraphTraversal<Vertex, Vertex> vertexTraversal, JsonObject json) {
        for (Map.Entry<String, JsonElement> keyValue : json.entrySet()) {
            vertexTraversal.property(keyValue.getKey(), keyValue.getValue().getAsString());
        }
    }

    private void addEdge(Vertex from, Vertex to) {
        this.graphTraversal.V(from).addE(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL).to(to).next();
    }

    private void addEdge(Vertex from, Vertex to, Object key, Object value, Object... keyValues) {
        this.graphTraversal.V(from).addE(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL).to(to).property(key, value,
                keyValues).next();
    }

    private void appendThreadEvent(String threadId, Vertex event, Event originalEvent) {
        try {
            Path lastThreadEventVertex = this.graphTraversal.V()
                    .has("threadId", threadId)
                    .has(GremlinExecutionGraph.EVENT_ID, P.neq(event.id()))
                    .has("kernelTime", P.lt(originalEvent.getKernelTime()))
                    .order().by("kernelTime", Order.decr)
                    .optional(__.outE(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL)
                            .inV()
                            .has("threadId", threadId))
                    .path()
                    .limit(1)
                    .next();

            if (lastThreadEventVertex.size() == 1) {
                Vertex lastThreadEvent = lastThreadEventVertex.get(0);
                this.graphTraversal.V(lastThreadEvent.id())
                        .addE(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL).to(event)
                        .next();
            } else {
                // Break the connection between two existing events and add two new connections.
                Vertex previousEvent = lastThreadEventVertex.get(0);
                Edge edgeBetweenPreviousAndNextEvents = lastThreadEventVertex.get(1);
                Vertex nextEvent = lastThreadEventVertex.get(2);

                this.graphTraversal.E(edgeBetweenPreviousAndNextEvents).drop().iterate();
                this.graphTraversal.V(previousEvent.id())
                        .addE(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL).to(event)
                        .inV()
                        .addE(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL).to(nextEvent)
                        .next();
            }
        } catch (NoSuchElementException e) {
            if (logger.isDebugEnabled())
                logger.debug("Thread " + threadId + " does not have any event yet. Couldn't append ["
                        + originalEvent.getId() + "].");
        }
    }

    @Override
    public void close() throws Exception {
        this.graphTraversal.close();
        this.graph.close();
    }
}

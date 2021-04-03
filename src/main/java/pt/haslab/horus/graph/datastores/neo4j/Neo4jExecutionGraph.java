package pt.haslab.horus.graph.datastores.neo4j;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.google.gson.JsonObject;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.summary.ResultSummary;
import pt.haslab.horus.FalconEventHandler;
import pt.haslab.horus.events.*;
import pt.haslab.horus.graph.AbstractExecutionGraph;
import pt.haslab.horus.graph.datastores.gremlin.GremlinExecutionGraph;
import pt.haslab.horus.graph.timeline.TimelineIdGeneratorFactory;

import java.util.*;
import java.util.stream.Collectors;

public class Neo4jExecutionGraph extends AbstractExecutionGraph implements AutoCloseable {
    public static String HAPPENS_BEFORE_LABEL = "HAPPENS_BEFORE";
    public static String EVENT_ID_PROPERTY = "eventId";
    private final static Logger logger = LogManager.getLogger(Neo4jExecutionGraph.class);
    private final Driver driver;
    private final Session session;
    private final MetricRegistry statsRegistry;
    private final EventParamsBuilder eventParamsBuilder;

    public Neo4jExecutionGraph(Driver driver) {
        this.driver = driver;
        this.session = this.driver.session();
        this.timelineIdGenerator = TimelineIdGeneratorFactory.get();
        this.statsRegistry = SharedMetricRegistries.getOrCreate("stats");
        this.eventParamsBuilder = new EventParamsBuilder(EVENT_ID_PROPERTY, this.timelineIdGenerator);
    }

    @Override
    public void addEvent(ProcessCreate event) {
        HashMap<String, Object> eventParams = this.eventParamsBuilder.buildParamsMap(event);

        this.session.writeTransaction(tx -> {
            Long eventNodeId = storeEventNode(tx, eventParams, event.getType().getLabel());

            // Connect to the corresponding START event.
            tx.run(
                    "MATCH (start:START) " +
                            "WHERE start.tid = $tid AND start.host = $host AND start.kernelTime >= $kernelTime AND NOT (:CREATE)-[]->(start)" +
                            "WITH start " +
                            "ORDER BY start.kernelTime ASC " +
                            "LIMIT 1 " +
                            "MATCH (create) " +
                            "WHERE ID(create) = $id " +
                            "WITH create, start " +
                            "CREATE (create)-[:" + HAPPENS_BEFORE_LABEL + "]->(start) ",
                    new HashMap<String, Object>() {{
                        put("tid", event.getChildPid());
                        put("host", event.getHost());
                        put("kernelTime", event.getKernelTime());
                        put("id", eventNodeId);
                    }});


            return eventNodeId;
        });
    }

    @Override
    public void addEvent(ProcessStart event) {
        HashMap<String, Object> eventParams = this.eventParamsBuilder.buildParamsMap(event);

        this.session.writeTransaction(tx -> {
            Long eventNodeId = storeEventNode(tx, eventParams, event.getType().getLabel());

            // Connect to the corresponding CREATE event.
            tx.run(
                    "MATCH (create:CREATE) " +
                            "WHERE create.childPid = $childPid AND create.host = $host AND create.kernelTime <= $kernelTime AND NOT (create)-[]->(:START)" +
                            "WITH create " +
                            "ORDER BY create.kernelTime ASC " +
                            "LIMIT 1 " +
                            "MATCH (start:START:EVENT) " +
                            "WHERE ID(start) = $id " +
                            "WITH create, start " +
                            "CREATE (create)-[:" + HAPPENS_BEFORE_LABEL + "]->(start) ",
                    new HashMap<String, Object>() {{
                        put("childPid", event.getTid());
                        put("host", event.getHost());
                        put("kernelTime", event.getKernelTime());
                        put("id", eventNodeId);
                    }});

            return eventNodeId;
        });
    }

    @Override
    public void addEvent(ProcessEnd event) {
        HashMap<String, Object> eventParams = this.eventParamsBuilder.buildParamsMap(event);

        this.session.writeTransaction(tx -> {
            Long eventNodeId = storeEventNode(tx, eventParams, event.getType().getLabel());

            // Connect to the corresponding JOIN event.
            tx.run(
                    "MATCH (join:JOIN) " +
                            "WHERE join.childPid = $childPid AND join.host = $host AND join.kernelTime >= $kernelTime AND NOT (:END)-[]->(join)" +
                            "WITH join " +
                            "ORDER BY join.kernelTime ASC " +
                            "LIMIT 1 " +
                            "MATCH (end) " +
                            "WHERE ID(end) = $id " +
                            "WITH end, join " +
                            "CREATE (end)-[:" + HAPPENS_BEFORE_LABEL + "]->(join) ",
                    new HashMap<String, Object>() {{
                        put("childPid", event.getTid());
                        put("host", event.getHost());
                        put("kernelTime", event.getKernelTime());
                        put("id", eventNodeId);
                    }});
            return eventNodeId;
        });
    }

    @Override
    public void addEvent(ProcessJoin event) {
        HashMap<String, Object> eventParams = this.eventParamsBuilder.buildParamsMap(event);

        this.session.writeTransaction(tx -> {
            Long eventNodeId = storeEventNode(tx, eventParams, event.getType().getLabel());

            // Connect to the corresponding END event.
            tx.run(
                    "MATCH (end:END) " +
                            "WHERE end.tid = $tid AND end.host = $host AND end.kernelTime <= $kernelTime AND NOT (end)-[]->(:JOIN)" +
                            "WITH end " +
                            "ORDER BY end.kernelTime ASC " +
                            "LIMIT 1 " +
                            "MATCH (join) " +
                            "WHERE ID(join) = $id " +
                            "WITH end, join " +
                            "CREATE (end)-[:" + HAPPENS_BEFORE_LABEL + "]->(join) ",
                    new HashMap<String, Object>() {{
                        put("tid", event.getChildPid());
                        put("host", event.getHost());
                        put("kernelTime", event.getKernelTime());
                        put("id", eventNodeId);
                    }});

            return eventNodeId;
        });
    }

    @Override
    public void addEvent(SocketAccept event) {
        HashMap<String, Object> eventParams = this.eventParamsBuilder.buildParamsMap(event);

        this.session.writeTransaction(tx -> {
            Long eventNodeId = storeEventNode(tx, eventParams, event.getType().getLabel());

            // Connect to the corresponding CONNECT event.
            tx.run(
                    "MATCH (connect:CONNECT) " +
                            "WHERE connect.socketId = $socketId AND connect.socketFamily = $socketFamily AND NOT (connect)-[]->(:ACCEPT) " +
                            "MATCH (accept) " +
                            "WHERE ID(accept) = $id " +
                            "WITH connect, accept " +
                            "CREATE (connect)-[:" + HAPPENS_BEFORE_LABEL + "]->(accept) ",
                    new HashMap<String, Object>() {{
                        put("socketId", event.getSocketId());
                        put("socketFamily", event.getSocketFamily());
                        put("id", eventNodeId);
                    }});

            return eventNodeId;
        });
    }

    @Override
    public void addEvent(SocketConnect event) {
        HashMap<String, Object> eventParams = this.eventParamsBuilder.buildParamsMap(event);

        this.session.writeTransaction(tx -> {
            Long eventNodeId = storeEventNode(tx, eventParams, event.getType().getLabel());

            // Connect to the corresponding ACCEPT event.
            tx.run(
                    "MATCH (accept:ACCEPT) " +
                            "WHERE accept.socketId = $socketId AND accept.socketFamily = $socketFamily AND NOT (:CONNECT)-[]->(accept) " +
                            "MATCH (connect) " +
                            "WHERE ID(connect) = $id " +
                            "WITH connect, accept " +
                            "CREATE (connect)-[:" + HAPPENS_BEFORE_LABEL + "]->(accept) ",
                    new HashMap<String, Object>() {{
                        put("socketId", event.getSocketId());
                        put("socketFamily", event.getSocketFamily());
                        put("id", eventNodeId);
                    }});

            return eventNodeId;
        });
    }

    @Override
    public void addEvent(SocketSend event) {
        HashMap<String, Object> eventParams = this.eventParamsBuilder.buildParamsMap(event);
        eventParams.put("_unmatchedSize", event.getSize());
        eventParams.put("_complete", false);

        this.session.writeTransaction(tx -> {
            Long eventNodeId = storeEventNode(tx, eventParams, event.getType().getLabel());

            // Connect to the corresponding RCV events.
            Timer.Context findMatchesMetricTimer = this.statsRegistry.timer(MetricRegistry.name(Neo4jExecutionGraph.class, "snd-find-matches")).time();
            StatementResult incompleteRcvs = tx.run(
                    "MATCH (rcv:RCV) " +
                            "WHERE rcv.socketFrom = $socketFrom AND rcv.socketFromPort = $socketFromPort " +
                            "AND rcv.socketTo = $socketTo AND rcv.socketToPort = $socketToPort AND rcv._complete = false " +
                            "RETURN ID(rcv) as rcvId, rcv.kernelTime as rcvKernelTime, rcv._unmatchedSize as rcvRemainingSize " +
                            "ORDER BY rcv.kernelTime ASC",
                    new HashMap<String, Object>() {{
                        put("socketFrom", event.getSocketFrom());
                        put("socketFromPort", event.getSourcePort());
                        put("socketTo", event.getSocketTo());
                        put("socketToPort", event.getDestinationPort());
                    }}
            );

            int remainingSndSize = event.getSize();
            List<Pair<Long, Integer>> matchedRcvs = new ArrayList<>();

            while (remainingSndSize > 0 && incompleteRcvs.hasNext()) {
                Record incompleteRcv = incompleteRcvs.next();

                Long rcvId = incompleteRcv.get("rcvId").asLong();
                Long rcvKernelTime = incompleteRcv.get("rcvKernelTime").asLong();
                int rcvRemainingSize = incompleteRcv.get("rcvRemainingSize").asInt();

                // Update remaining unmatched size.
                int matchingSize = Math.min(rcvRemainingSize, remainingSndSize);
                remainingSndSize = remainingSndSize - matchingSize;

                // Add incomplete RCV to list of RCV events that should be connected to this SND.
                matchedRcvs.add(Pair.of(rcvId, matchingSize));
            }

            findMatchesMetricTimer.stop();

            if (matchedRcvs.size() > 0) {
                /*
                 * The output of the code bellow is the following query:
                 *
                 * MATCH (snd:SND) WHERE ID(snd) = $sndId
                 * MATCH (rcv0:RCV) WHERE ID(rcv0) = $rcv0Id
                 * MATCH (rcv1:RCV) WHERE ID(rcv1) = $rcv1Id
                 * ...
                 * MATCH (rcvN:RCV) WHERE ID(rcv1) = $rcvNId
                 *
                 * CREATE (snd)-[:HAPPENS_BEFORE]->(rcv0)
                 * CREATE (snd)-[:HAPPENS_BEFORE]->(rcv1)
                 * ...
                 * CREATE (snd)-[:HAPPENS_BEFORE]->(rcvN)
                 */

                StringBuilder matches = new StringBuilder();
                StringBuilder creates = new StringBuilder();
                HashMap<String, Object> relationshipProps = new HashMap<String, Object>();

                matches.append("MATCH (snd) WHERE ID(snd) = $sndId\n");
                relationshipProps.put("sndId", eventNodeId);

                int currentId = 0;
                for (Pair<Long, Integer> matchedRcv : matchedRcvs) {
                    matches.append("MATCH (rcv" + currentId + ") WHERE ID(rcv" + currentId + ") = $rcv" + currentId + "Id\n");

                    creates.append("CREATE (snd)-[:" + HAPPENS_BEFORE_LABEL + "]->(rcv" + currentId + ")\n");
                    creates.append("SET rcv" + currentId + "._unmatchedSize = rcv" + currentId + "._unmatchedSize - $readSizeSndRcv" + currentId + "\n");
                    creates.append("SET rcv" + currentId + "._complete = (rcv" + currentId + "._unmatchedSize = 0)\n");

                    relationshipProps.put("rcv" + currentId + "Id", matchedRcv.getLeft());
                    relationshipProps.put("readSizeSndRcv" + currentId, matchedRcv.getRight());

                    currentId++;
                }

                creates.append("SET snd._unmatchedSize = $remainingSndSize\n");
                creates.append("SET snd._complete = (snd._unmatchedSize = 0)\n");
                relationshipProps.put("remainingSndSize", remainingSndSize);

                tx.run(matches.append(creates).toString(), relationshipProps);
            }


            return eventNodeId;
        });
    }

    @Override
    public void addEvent(SocketReceive event) {
        HashMap<String, Object> eventParams = this.eventParamsBuilder.buildParamsMap(event);
        eventParams.put("_unmatchedSize", event.getSize());
        eventParams.put("_complete", false);

        this.session.writeTransaction(tx -> {
            Long eventNodeId = storeEventNode(tx, eventParams, event.getType().getLabel());

            // Connect to the corresponding SND events.
            Timer.Context findMatchesMetricTimer = this.statsRegistry.timer(MetricRegistry.name(Neo4jExecutionGraph.class, "rcv-find-matches")).time();
            StatementResult incompleteSnds = tx.run(
                    "MATCH (snd:SND) " +
                            "WHERE snd.socketFrom = $socketFrom AND snd.socketFromPort = $socketFromPort " +
                            "AND snd.socketTo = $socketTo AND snd.socketToPort = $socketToPort AND snd._complete = false " +
                            "RETURN ID(snd) as sndId, snd.kernelTime as sndKernelTime, snd._unmatchedSize as sndRemainingSize " +
                            "ORDER BY snd.kernelTime ASC",
                    new HashMap<String, Object>() {{
                        put("socketFrom", event.getSocketFrom());
                        put("socketFromPort", event.getSourcePort());
                        put("socketTo", event.getSocketTo());
                        put("socketToPort", event.getDestinationPort());
                    }}
            );

            findMatchesMetricTimer.stop();

            int remainingRcvSize = event.getSize();
            List<Pair<Long, Integer>> matchedSnds = new ArrayList<>();

            while (remainingRcvSize > 0 && incompleteSnds.hasNext()) {
                Record incompleteSnd = incompleteSnds.next();

                Long sndId = incompleteSnd.get("sndId").asLong();
                Long sndKernelTime = incompleteSnd.get("sndKernelTime").asLong();
                int sndRemainingSize = incompleteSnd.get("sndRemainingSize").asInt();

                // Update remaining unmatched size.
                int matchingSize = Math.min(sndRemainingSize, remainingRcvSize);
                remainingRcvSize = remainingRcvSize - matchingSize;

                // Add incomplete SND to list of SND events that should be connected to this RCV.
                matchedSnds.add(Pair.of(sndId, matchingSize));
            }


            if (matchedSnds.size() > 0) {
                /*
                 * The output of the code bellow is the following query:
                 *
                 * MATCH (rcv:RCV) WHERE ID(rcv) = $rcvId
                 * MATCH (snd0:SND) WHERE ID(snd0) = $snd0Id
                 * MATCH (snd1:SND) WHERE ID(snd1) = $snd1Id
                 * ...
                 * MATCH (sndN:SND) WHERE ID(snd1) = $sndNId
                 *
                 * CREATE (snd0)-[:HAPPENS_BEFORE]->(rcv)
                 * CREATE (snd1)-[:HAPPENS_BEFORE]->(rcv)
                 * ...
                 * CREATE (sndN)-[:HAPPENS_BEFORE]->(rcv)
                 */

                StringBuilder matches = new StringBuilder();
                StringBuilder creates = new StringBuilder();
                HashMap<String, Object> relationshipProps = new HashMap<String, Object>();

                matches.append("MATCH (rcv) WHERE ID(rcv) = $rcvId\n");
                relationshipProps.put("rcvId", eventNodeId);

                int currentId = 0;
                for (Pair<Long, Integer> matchedSnd : matchedSnds) {
                    matches.append("MATCH (snd" + currentId + ") WHERE ID(snd" + currentId + ") = $snd" + currentId + "Id\n");

                    creates.append("CREATE (snd" + currentId + ")-[:" + HAPPENS_BEFORE_LABEL + "]->(rcv)\n");
                    creates.append("SET snd" + currentId + "._unmatchedSize = snd" + currentId + "._unmatchedSize - $readSizeSnd" + currentId + "Rcv\n");
                    creates.append("SET snd" + currentId + "._complete = (snd" + currentId + "._unmatchedSize = 0)\n");

                    relationshipProps.put("snd" + currentId + "Id", matchedSnd.getLeft());
                    relationshipProps.put("readSizeSnd" + currentId + "Rcv", matchedSnd.getRight());

                    currentId++;
                }

                creates.append("SET rcv._unmatchedSize = $remainingRcvSize\n");
                creates.append("SET rcv._complete = (rcv._unmatchedSize = 0)\n");
                relationshipProps.put("remainingRcvSize", remainingRcvSize);

                tx.run(matches.append(creates).toString(), relationshipProps);
            }

            return eventNodeId;
        });
    }

    @Override
    public void addEvent(Log event) {
        HashMap<String, Object> eventParams = this.eventParamsBuilder.buildParamsMap(event);

        this.session.writeTransaction(tx -> {
            Long eventNodeId = storeEventNode(tx, eventParams, event.getType().getLabel());

            return eventNodeId;
        });
    }

    @Override
    public void addEvent(FSync event) {
        HashMap<String, Object> eventParams = this.eventParamsBuilder.buildParamsMap(event);

        this.session.writeTransaction(tx -> {
            Long eventNodeId = storeEventNode(tx, eventParams, event.getType().getLabel());

            return eventNodeId;
        });
    }

    @Override
    public void addEvent(Event event) {
        if (event instanceof ProcessCreate)
            this.addEvent((ProcessCreate) event);

        else if (event instanceof ProcessStart)
            this.addEvent((ProcessStart) event);

        else if (event instanceof ProcessJoin)
            this.addEvent((ProcessJoin) event);

        else if (event instanceof ProcessEnd)
            this.addEvent((ProcessEnd) event);

        else if (event instanceof SocketConnect)
            this.addEvent((SocketConnect) event);

        else if (event instanceof SocketAccept)
            this.addEvent((SocketAccept) event);

        else if (event instanceof SocketSend)
            this.addEvent((SocketSend) event);

        else if (event instanceof SocketReceive)
            this.addEvent((SocketReceive) event);

        else if (event instanceof Log)
            this.addEvent((Log) event);

        else if (event instanceof FSync)
            this.addEvent((FSync) event);

        else
            throw new IllegalArgumentException("Unrecognized event type.");
    }

    @Override
    public void storeEvents(List<Event> events) {
        List<HashMap<String, Object>> eventsParams = events.stream().map((event) -> {
            HashMap<String, Object> params = this.eventParamsBuilder.buildParamsMap(event);
            params.put("type", event.getType().getLabel());

            return params;
        }).collect(Collectors.toList());

        this.driver.session().run(
            "UNWIND $events AS event " +
                    "MERGE (n:EVENT {eventId: event.eventId}) " +
                    "WITH n, event as fields " +
                    "CALL apoc.create.addLabels(n, [fields.type]) YIELD node " +
                    "SET node = fields",
            new HashMap<String, Object>() {{
                put("events", eventsParams);
            }}
        ).consume();
    }

    @Override
    public void storeRelationships(Collection<Pair<Event, Event>> relationships) {
        List<HashMap<String, String>> relationshipsParams = relationships.stream().map((relationship) -> {
            HashMap<String, String> params = new HashMap<>();
            params.put("from", relationship.getLeft().getId());
            params.put("to", relationship.getRight().getId());

            return params;
        }).collect(Collectors.toList());

        this.driver.session().run(
            "UNWIND $relationships AS relationship " +
                    "MERGE (from:EVENT {eventId: relationship.from}) " +
                    "MERGE (to:EVENT {eventId: relationship.to}) " +
                    "CREATE(from)-[:HAPPENS_BEFORE]->(to)",
            new HashMap<String, Object>() {{
                put("relationships", relationshipsParams);
            }}
        ).consume();



    }

    public void clearGraph() {
        this.session.writeTransaction(tx -> {
            tx.run("MATCH (n) DETACH DELETE n");

            return null;
        });
    }

    @Override
    public void close() throws Exception {
        this.driver.close();
    }

    public Session getSession() {
        return session;
    }

    private Long storeEventNode(Transaction tx, HashMap<String, Object> eventParams, String additionalLabels) {
        Timer.Context insertEventMetricTimer = this.statsRegistry.timer(MetricRegistry.name(Neo4jExecutionGraph.class, "insert-event")).time();
        String query;
        Map<String, Object> queryParams;
        String eventLabels = additionalLabels == null ? "" : ":" + additionalLabels;

        // Add "created_at" attribute.
        eventParams.put("created_at", System.currentTimeMillis());

        StatementResult previousThreadEvent = tx.run(
                "MATCH (prevEvent:EVENT) " +
                        "WHERE prevEvent.kernelTime <= $kernelTime " +
                        "AND prevEvent.threadId = $threadId " +
                        "RETURN ID(prevEvent) as prevEventId ORDER BY prevEvent.kernelTime DESC LIMIT 1",
                new HashMap<String, Object>() {{
                    put("threadId", eventParams.get("threadId"));
                    put("kernelTime", eventParams.get("kernelTime"));
                }});

        boolean hasPreviousEvent = previousThreadEvent.hasNext();
        Value prevEventId = hasPreviousEvent ? previousThreadEvent.single().get("prevEventId") : null;

        StatementResult nextThreadEvent = tx.run(
                "MATCH (nextEvent:EVENT) " +
                        "WHERE nextEvent.kernelTime > $kernelTime " +
                        "AND nextEvent.threadId = $threadId " +
                        "RETURN ID(nextEvent) as nextEventId ORDER BY nextEvent.kernelTime ASC LIMIT 1",
                new HashMap<String, Object>() {{
                    put("threadId", eventParams.get("threadId"));
                    put("kernelTime", eventParams.get("kernelTime"));
                }});

        boolean hasNextEvent = nextThreadEvent.hasNext();
        Value nextEventId = hasNextEvent ? nextThreadEvent.single().get("nextEventId") : null;

        if (!hasPreviousEvent && !hasNextEvent) {
            query = "CREATE (n $props) " +
                    "SET n:EVENT" + eventLabels + " " +
                    "RETURN ID(n) as id";

            queryParams = new HashMap<String, Object>() {{
                put("props", eventParams);
            }};
        } else if (hasPreviousEvent && !hasNextEvent) {
            // When there's no next event, we just append the current event to
            // the latest event of the same thread (same thread identifier).
            // We will execute a new Cypher query to store this new event.
            query = "MATCH (prevEvent) " +
                    "WHERE ID(prevEvent) = $prevEventId " +
                    "CREATE (prevEvent)-[:" + HAPPENS_BEFORE_LABEL + "]->(n $props) " +
                    "SET n:EVENT" + eventLabels + " " +
                    "RETURN ID(n) as id";

            queryParams = new HashMap<String, Object>() {{
                put("prevEventId", prevEventId.asLong());
                put("props", eventParams);
            }};
        } else if (!hasPreviousEvent) {
            logger.info("A past event is about to be prepended to a future event. Event [" + eventParams.get("eventId") + "] with labels ["+eventLabels+"]");

            // When there's no previous event, we just append the current event to
            // the future event of the same thread (same thread identifier).
            // We will execute a new Cypher query to store this new event.
            query = "MATCH (nextEvent) " +
                    "WHERE ID(nextEvent) = $nextEventId " +
                    "CREATE (n $props)-[:" + HAPPENS_BEFORE_LABEL + "]->(nextEvent) " +
                    "SET n:EVENT" + eventLabels + " " +
                    "RETURN ID(n) as id";

            queryParams = new HashMap<String, Object>() {{
                put("nextEventId", nextEventId.asLong());
                put("props", eventParams);
            }};
        } else {
            logger.info("A past event is about to be inserted between events. Event [" + eventParams.get("eventId") + "] with labels ["+eventLabels+"]");

            tx.run(
                    "OPTIONAL MATCH (prevEvent)-[hb:" + HAPPENS_BEFORE_LABEL + "]->(nextEvent) " +
                            "WHERE ID(prevEvent) = $prevEventId AND ID(nextEvent) = $nextEventId " +
                            "DELETE hb",
                    new HashMap<String, Object>() {{
                        put("prevEventId", prevEventId.asLong());
                        put("nextEventId", nextEventId.asLong());
                    }}).consume();

            query = "MATCH (prevEvent), (nextEvent) " +
                    "WHERE ID(prevEvent) = $prevEventId AND ID(nextEvent) = $nextEventId " +
                    "CREATE (prevEvent)-[:" + HAPPENS_BEFORE_LABEL + "]->(n $props)-[:" + HAPPENS_BEFORE_LABEL + "]->(nextEvent) " +
                    "SET n:EVENT" + eventLabels + " " +
                    "RETURN ID(n) as id";

            queryParams = new HashMap<String, Object>() {{
                put("prevEventId", prevEventId.asLong());
                put("nextEventId", nextEventId.asLong());
                put("props", eventParams);
            }};
        }

        Record record = tx.run(query, queryParams).single();

        insertEventMetricTimer.stop();

        return record.get("id").asLong();
    }
}

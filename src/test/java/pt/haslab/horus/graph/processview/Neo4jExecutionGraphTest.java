package pt.haslab.horus.graph.processview;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.*;
import org.neo4j.driver.v1.*;
import pt.haslab.horus.BaseTest;
import pt.haslab.horus.Config;
import pt.haslab.horus.events.*;
import pt.haslab.horus.graph.datastores.neo4j.Neo4jExecutionGraph;
import pt.haslab.horus.graph.timeline.ProcessTimelineIdGenerator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class Neo4jExecutionGraphTest
        extends BaseTest {

    private static Driver driver;
    private static Neo4jExecutionGraph graph;

    @BeforeAll
    static void openGraph() {
        String uri = Config.getInstance().getConfig().getString("neo4j.uri");
        String user = Config.getInstance().getConfig().getString("neo4j.user");
        String password = Config.getInstance().getConfig().getString("neo4j.password");

        AuthToken authToken = user == null ? AuthTokens.none() : AuthTokens.basic(user, password);
        driver = GraphDatabase.driver(uri, authToken);

        graph = new Neo4jExecutionGraph(driver);
        graph.setTimelineIdGenerator(new ProcessTimelineIdGenerator());
        graph.clearGraph();
    }

    @AfterAll
    static void closeGraph() {
        try {
            graph.close();
            driver.close();
        } catch (Exception e) {
            // Ignore exception.
        }
    }

    @AfterEach
    void cleanGraph() {
        graph.clearGraph();
    }

    /**
     * Disabled test.
     * This case is already handled by tracer.
     */
    @Disabled
    void eventsWithSameKernelTimeAreCorrectlyAppendedToSameProcessTimeline() {

        ProcessCreate create1 = new ProcessCreate();
        create1.setId("create-1");
        create1.setUserTime(1);
        create1.setKernelTime(1);
        create1.setPid(9);
        create1.setTid(9);
        create1.setComm("comm");
        create1.setHost("host");
        create1.setChildPid(10);
        graph.addEvent(create1);

        ProcessStart start1 = new ProcessStart();
        start1.setId("start-1");
        start1.setUserTime(1);
        start1.setKernelTime(1);
        start1.setPid(9);
        start1.setTid(10);
        start1.setComm("comm");
        start1.setHost("host");
        graph.addEvent(start1);

        ProcessCreate create2 = new ProcessCreate();
        create2.setId("create-2");
        create2.setUserTime(1);
        create2.setKernelTime(2);
        create2.setPid(9);
        create2.setTid(9);
        create2.setComm("comm");
        create2.setHost("host");
        create2.setChildPid(11);
        graph.addEvent(create2);

        ProcessStart start2 = new ProcessStart();
        start2.setId("start-2");
        start2.setUserTime(1);
        start2.setKernelTime(2);
        start2.setPid(9);
        start2.setTid(11);
        start2.setComm("comm");
        start2.setHost("host");
        graph.addEvent(start2);

        // Expected: (create-1)-->(start-1)-->(create-2)-->(start-2)
        try {
            Record record = graph.getSession().run(
                    "MATCH (create1:CREATE:EVENT)-[:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]-(start1:START:EVENT)-[:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]-(create2:CREATE:EVENT)-[:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]->(start2:START:EVENT) " +
                            "WHERE create1.eventId = 'create-1' AND start1.eventId = 'start-1' AND create2.eventId = 'create-2' AND start2.eventId = 'start-2' " +
                            "RETURN 'OK'"
            ).single();

            assertEquals("OK", record.get(0).asString());
        } catch (NoSuchElementException e) {
            fail("Couldn't find the expected path.");
        }
    }

    /**
     * This test is a regression test. We've detected a strange situation happening
     * with LOG and CREATE events. Namely, when LOG is the first event, Consumer creates
     * another thread for the same ThreadId, but with distinct tid. That means that LOG and
     * CREATE are both root events of the same timeline.
     */
    @Test
    void logEventAndCreateEventsAreAppendedInTheCorrectOrder() {

        Log log = new Log();
        log.setId("log-1");
        log.setUserTime(1);
        log.setKernelTime(2);
        log.setPid(9);
        log.setTid(10);
        log.setComm("comm");
        log.setHost("host");
        graph.addEvent(log);

        ProcessCreate create1 = new ProcessCreate();
        create1.setId("create-1");
        create1.setUserTime(1);
        create1.setKernelTime(1);
        create1.setPid(9);
        create1.setTid(10);
        create1.setComm("comm");
        create1.setHost("host");
        create1.setChildPid(11);
        graph.addEvent(create1);

        ProcessStart start1 = new ProcessStart();
        start1.setId("start-1");
        start1.setUserTime(1);
        start1.setKernelTime(2);
        start1.setPid(11);
        start1.setTid(11);
        start1.setComm("comm");
        start1.setHost("host");
        graph.addEvent(start1);

        try {
            // Expected: (create-1)-->(start-1)
            Record record;
            record = graph.getSession().run(
                    "MATCH (create1:CREATE:EVENT)-[:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]-(start1:START:EVENT) " +
                            "WHERE create1.eventId = 'create-1' AND start1.eventId = 'start-1' " +
                            "RETURN 'OK'"
            ).single();

            assertEquals("OK", record.get(0).asString());

            // Expected: (create-1)-->(log-1)
            record = graph.getSession().run(
                    "MATCH (create1:CREATE:EVENT)-[:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]-(log1:LOG:EVENT) " +
                            "WHERE create1.eventId = 'create-1' AND log1.eventId = 'log-1' " +
                            "RETURN 'OK'"
            ).single();

            assertEquals("OK", record.get(0).asString());
        } catch (NoSuchElementException e) {
            fail("Couldn't find the expected path.");
        }
    }

    @Test
    public void insertEventsAndRelationshipsBatch() {
        Log log = new Log();
        log.setId("log-1");
        log.setUserTime(1);
        log.setKernelTime(2);
        log.setPid(9);
        log.setTid(10);
        log.setComm("comm");
        log.setHost("host");

        ProcessCreate create1 = new ProcessCreate();
        create1.setId("create-1");
        create1.setUserTime(1);
        create1.setKernelTime(1);
        create1.setPid(9);
        create1.setTid(10);
        create1.setComm("comm");
        create1.setHost("host");
        create1.setChildPid(11);

        ProcessStart start1 = new ProcessStart();
        start1.setId("start-1");
        start1.setUserTime(1);
        start1.setKernelTime(2);
        start1.setPid(11);
        start1.setTid(11);
        start1.setComm("comm");
        start1.setHost("host");

        graph.storeEvents(new ArrayList<Event>() {{
            add(log);
            add(create1);
            add(start1);
        }});

        graph.storeRelationships(new ArrayList<Pair<Event,Event>>() {{
            add(new ImmutablePair<>(create1, start1));
            add(new ImmutablePair<>(create1, log));
        }});

        try {
            // Expected: (create-1)-->(start-1)
            Record record;
            record = graph.getSession().run(
                    "MATCH (create1:CREATE:EVENT)-[:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]-(start1:START:EVENT) " +
                            "WHERE create1.eventId = 'create-1' AND start1.eventId = 'start-1' " +
                            "RETURN 'OK'"
            ).single();

            assertEquals("OK", record.get(0).asString());

            // Expected: (create-1)-->(log-1)
            record = graph.getSession().run(
                    "MATCH (create1:CREATE:EVENT)-[:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]-(log1:LOG:EVENT) " +
                            "WHERE create1.eventId = 'create-1' AND log1.eventId = 'log-1' " +
                            "RETURN 'OK'"
            ).single();

            assertEquals("OK", record.get(0).asString());
        } catch (NoSuchElementException e) {
            fail("Couldn't find the expected path.");
        }
    }
}
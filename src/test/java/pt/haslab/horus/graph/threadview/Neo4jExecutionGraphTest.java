package pt.haslab.horus.graph.threadview;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.v1.*;
import pt.haslab.horus.BaseTest;
import pt.haslab.horus.Config;
import pt.haslab.horus.events.*;
import pt.haslab.horus.graph.datastores.neo4j.Neo4jExecutionGraph;

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

    @Test
    void addEventAppendsCorrectlyBetweenTwoExistingEventsInThreadTimeline() {

        // Prepare scenario:
        // create-1: (Thread-9) --creates--> (Thread-10)
        // create-2: (Thread-9) --creates--> (Thread-11)
        // create-3: (Thread-9) --creates--> (Thread-12)

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

        ProcessCreate create3 = new ProcessCreate();
        create3.setId("create-3");
        create3.setUserTime(1);
        create3.setKernelTime(3);
        create3.setPid(9);
        create3.setTid(9);
        create3.setComm("comm");
        create3.setHost("host");
        create3.setChildPid(12);
        graph.addEvent(create3);

        // Insert create-2 later than others.
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

        // Expected: (create-1)-->(create-2)-->(create-3)
        try {
            Record record = graph.getSession().run(
                    "MATCH (create1:CREATE:EVENT)-[:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]-(create2:CREATE:EVENT)-[:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]->(create3:CREATE:EVENT) " +
                            "WHERE create1.eventId = 'create-1' AND create2.eventId = 'create-2' AND create3.eventId = 'create-3' " +
                            "RETURN 'OK'"
            ).single();

            assertEquals("OK", record.get(0).asString());
        } catch (NoSuchElementException e) {
            fail("Couldn't find the expected path.");
        }
    }

    @Test
    void addProcessCreateAppendsToExistingThreadTimeline() {

        // Prepare scenario:
        // create-1: (Thread-9) --creates--> (Thread-10)
        // create-2: (Thread-10) --creates--> (Thread-11)
        // create-3: (Thread-10) --creates--> (Thread-12)

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

        ProcessCreate create2 = new ProcessCreate();
        create2.setId("create-2");
        create2.setUserTime(1);
        create2.setKernelTime(2);
        create2.setPid(10);
        create2.setTid(10);
        create2.setComm("comm");
        create2.setHost("host");
        create2.setChildPid(11);
        graph.addEvent(create2);

        ProcessCreate create3 = new ProcessCreate();
        create3.setId("create-3");
        create3.setUserTime(1);
        create3.setKernelTime(3);
        create3.setPid(10);
        create3.setTid(10);
        create3.setComm("comm");
        create3.setHost("host");
        create3.setChildPid(12);
        graph.addEvent(create3);

        // Expected: (create-2)-->(create-3)
        try {
            Record record = graph.getSession().run(
                    "MATCH (create2:CREATE:EVENT)-[:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]->(create3:CREATE:EVENT) " +
                            "WHERE create2.eventId = 'create-2' AND create3.eventId = 'create-3' " +
                            "RETURN 'OK'"
            ).single();

            assertEquals("OK", record.get(0).asString());
        } catch (NoSuchElementException e) {
            fail("Couldn't find the expected path.");
        }
    }

    @Test
    void addProcessCreateAppendsToExistingStartEvent() {

        ProcessStart startEvent = new ProcessStart();
        startEvent.setId("start-1");
        startEvent.setUserTime(1);
        startEvent.setKernelTime(2);
        startEvent.setPid(9);
        startEvent.setTid(10);
        startEvent.setComm("comm");
        startEvent.setHost("host");
        graph.addEvent(startEvent);

        ProcessCreate createEvent = new ProcessCreate();
        createEvent.setId("create-1");
        createEvent.setUserTime(1);
        createEvent.setKernelTime(1);
        createEvent.setPid(9);
        createEvent.setTid(9);
        createEvent.setComm("comm");
        createEvent.setHost("host");
        createEvent.setChildPid(10);
        graph.addEvent(createEvent);

        // Expected: (create-1)-->(start-1)
        try {
            Record record = graph.getSession().run(
                    "MATCH (create1:CREATE:EVENT)-[:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]->(start1:START:EVENT) " +
                            "WHERE create1.eventId = 'create-1' AND start1.eventId = 'start-1' " +
                            "RETURN 'OK'"
            ).single();

            assertEquals("OK", record.get(0).asString());
        } catch (NoSuchElementException e) {
            fail("Couldn't find the expected path.");
        }
    }

    @Test
    void addProcessStartAppendsToExistingCreateEvent() {

        ProcessCreate createEvent = new ProcessCreate();
        createEvent.setId("create-1");
        createEvent.setUserTime(1);
        createEvent.setKernelTime(1);
        createEvent.setPid(9);
        createEvent.setTid(9);
        createEvent.setComm("comm");
        createEvent.setHost("host");
        createEvent.setChildPid(10);
        graph.addEvent(createEvent);

        ProcessStart startEvent = new ProcessStart();
        startEvent.setId("start-1");
        startEvent.setUserTime(1);
        startEvent.setKernelTime(2);
        startEvent.setPid(9);
        startEvent.setTid(10);
        startEvent.setComm("comm");
        startEvent.setHost("host");
        graph.addEvent(startEvent);

        // Expected: (create-1)-->(start-1)
        try {
            Record record = graph.getSession().run(
                    "MATCH (create1:CREATE:EVENT)-[:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]->(start1:START:EVENT) " +
                            "WHERE create1.eventId = 'create-1' AND start1.eventId = 'start-1' " +
                            "RETURN 'OK'"
            ).single();

            assertEquals("OK", record.get(0).asString());
        } catch (NoSuchElementException e) {
            fail("Couldn't find the expected path.");
        }
    }

    @Test
    void addProcessEndAppendsToExistingJoinEvent() {

        ProcessJoin joinEvent = new ProcessJoin();
        joinEvent.setId("join-1");
        joinEvent.setUserTime(1);
        joinEvent.setKernelTime(2);
        joinEvent.setPid(9);
        joinEvent.setTid(9);
        joinEvent.setComm("comm");
        joinEvent.setHost("host");
        joinEvent.setChildPid(10);
        graph.addEvent(joinEvent);

        ProcessEnd endEvent = new ProcessEnd();
        endEvent.setId("end-1");
        endEvent.setUserTime(1);
        endEvent.setKernelTime(1);
        endEvent.setPid(9);
        endEvent.setTid(10);
        endEvent.setComm("comm");
        endEvent.setHost("host");
        graph.addEvent(endEvent);

        // Expected: (end-1)-->(join-1)
        try {
            Record record = graph.getSession().run(
                    "MATCH (end1:END:EVENT)-[:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]->(join1:JOIN:EVENT) " +
                            "WHERE end1.eventId = 'end-1' AND join1.eventId = 'join-1' " +
                            "RETURN 'OK'"
            ).single();

            assertEquals("OK", record.get(0).asString());
        } catch (NoSuchElementException e) {
            fail("Couldn't find the expected path.");
        }
    }

    @Test
    void addProcessJoinAppendsToExistingEndEvent() {

        ProcessEnd endEvent = new ProcessEnd();
        endEvent.setId("end-1");
        endEvent.setUserTime(1);
        endEvent.setKernelTime(1);
        endEvent.setPid(9);
        endEvent.setTid(10);
        endEvent.setComm("comm");
        endEvent.setHost("host");
        graph.addEvent(endEvent);

        ProcessJoin joinEvent = new ProcessJoin();
        joinEvent.setId("join-1");
        joinEvent.setUserTime(1);
        joinEvent.setKernelTime(2);
        joinEvent.setPid(9);
        joinEvent.setTid(9);
        joinEvent.setComm("comm");
        joinEvent.setHost("host");
        joinEvent.setChildPid(10);
        graph.addEvent(joinEvent);

        // Expected: (end-1)-->(join-1)
        try {
            Record record = graph.getSession().run(
                    "MATCH (end1:END:EVENT)-[:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]->(join1:JOIN:EVENT) " +
                            "WHERE end1.eventId = 'end-1' AND join1.eventId = 'join-1' " +
                            "RETURN 'OK'"
            ).single();

            assertEquals("OK", record.get(0).asString());
        } catch (NoSuchElementException e) {
            fail("Couldn't find the expected path.");
        }
    }

    @Test
    void addSocketConnectAppendsToExistingAcceptEvent() {

        SocketAccept acceptEvent = new SocketAccept();
        acceptEvent.setId("accept-1");
        acceptEvent.setUserTime(1);
        acceptEvent.setKernelTime(2);
        acceptEvent.setPid(9);
        acceptEvent.setTid(9);
        acceptEvent.setComm("comm");
        acceptEvent.setHost("localhost");
        acceptEvent.setSocketId("socket-test-id");
        acceptEvent.setSocketFamily(10);
        acceptEvent.setSocketFrom("0.0.0.0");
        acceptEvent.setSourcePort(10);
        acceptEvent.setSocketTo("0.0.0.0");
        acceptEvent.setDestinationPort(10);
        graph.addEvent(acceptEvent);

        SocketConnect connectEvent = new SocketConnect();
        connectEvent.setId("connect-1");
        connectEvent.setUserTime(1);
        connectEvent.setKernelTime(2);
        connectEvent.setPid(9);
        connectEvent.setTid(10);
        connectEvent.setComm("comm");
        connectEvent.setHost("localhost");
        connectEvent.setSocketId("socket-test-id");
        connectEvent.setSocketFamily(10);
        connectEvent.setSocketFrom("0.0.0.0");
        connectEvent.setSourcePort(10);
        connectEvent.setSocketTo("0.0.0.0");
        connectEvent.setDestinationPort(10);
        graph.addEvent(connectEvent);

        // Expected: (connect-1)-->(accept-1)
        try {
            Record record = graph.getSession().run(
                    "MATCH (connect1:CONNECT:EVENT)-[:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]->(accept1:ACCEPT:EVENT) " +
                            "WHERE connect1.eventId = 'connect-1' AND accept1.eventId = 'accept-1' " +
                            "RETURN 'OK'"
            ).single();

            assertEquals("OK", record.get(0).asString());
        } catch (NoSuchElementException e) {
            fail("Couldn't find the expected path.");
        }
    }

    @Test
    void addSocketAcceptAppendsToExistingConnectEvent() {

        SocketConnect connectEvent = new SocketConnect();
        connectEvent.setId("connect-1");
        connectEvent.setUserTime(1);
        connectEvent.setKernelTime(2);
        connectEvent.setPid(9);
        connectEvent.setTid(10);
        connectEvent.setComm("comm");
        connectEvent.setHost("localhost");
        connectEvent.setSocketId("socket-test-id");
        connectEvent.setSocketFamily(10);
        connectEvent.setSocketFrom("0.0.0.0");
        connectEvent.setSourcePort(10);
        connectEvent.setSocketTo("0.0.0.0");
        connectEvent.setDestinationPort(10);
        graph.addEvent(connectEvent);

        SocketAccept acceptEvent = new SocketAccept();
        acceptEvent.setId("accept-1");
        acceptEvent.setUserTime(1);
        acceptEvent.setKernelTime(2);
        acceptEvent.setPid(9);
        acceptEvent.setTid(9);
        acceptEvent.setComm("comm");
        acceptEvent.setHost("localhost");
        acceptEvent.setSocketId("socket-test-id");
        acceptEvent.setSocketFamily(10);
        acceptEvent.setSocketFrom("0.0.0.0");
        acceptEvent.setSourcePort(10);
        acceptEvent.setSocketTo("0.0.0.0");
        acceptEvent.setDestinationPort(10);
        graph.addEvent(acceptEvent);

        // Expected: (connect-1)-->(accept-1)
        try {
            Record record = graph.getSession().run(
                    "MATCH (connect1:CONNECT:EVENT)-[:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]->(accept1:ACCEPT:EVENT) " +
                            "WHERE connect1.eventId = 'connect-1' AND accept1.eventId = 'accept-1' " +
                            "RETURN 'OK'"
            ).single();

            assertEquals("OK", record.get(0).asString());
        } catch (NoSuchElementException e) {
            fail("Couldn't find the expected path.");
        }
    }

    @Test
    void addSocketSendAppendsToExistingReceiveEvents() {
        HashMap<String, Object> rcv1 = new HashMap<String, Object>() {{
            put(Neo4jExecutionGraph.EVENT_ID_PROPERTY, "rcv-1");
            put("threadId", "9/9@host1");
            put("size", 1);
            put("kernelTime", 1);
            put("socketId", "socket-id");
            put("socketFamily", 10);
            put("socketFrom", "host2");
            put("socketFromPort", 10);
            put("socketTo", "host1");
            put("socketToPort", 10);
            put("_unmatchedSize", 0);
            put("_complete", true);
        }};

        HashMap<String, Object> rcv2 = new HashMap<String, Object>() {{
            put(Neo4jExecutionGraph.EVENT_ID_PROPERTY, "rcv-2");
            put("threadId", "9/9@host1");
            put("size", 5);
            put("kernelTime", 2);
            put("socketId", "socket-id");
            put("socketFamily", 10);
            put("socketFrom", "host2");
            put("socketFromPort", 10);
            put("socketTo", "host1");
            put("socketToPort", 10);
            put("_unmatchedSize", 0);
            put("_complete", true);
        }};

        HashMap<String, Object> rcv3 = new HashMap<String, Object>() {{
            put(Neo4jExecutionGraph.EVENT_ID_PROPERTY, "rcv-3");
            put("threadId", "9/9@host1");
            put("size", 5);
            put("kernelTime", 3);
            put("socketId", "socket-id");
            put("socketFamily", 10);
            put("socketFrom", "host2");
            put("socketFromPort", 10);
            put("socketTo", "host1");
            put("socketToPort", 10);
            put("_unmatchedSize", 4);
            put("_complete", false);
        }};

        HashMap<String, Object> rcv4 = new HashMap<String, Object>() {{
            put(Neo4jExecutionGraph.EVENT_ID_PROPERTY, "rcv-4");
            put("threadId", "9/9@host1");
            put("size", 5);
            put("kernelTime", 4);
            put("socketId", "socket-id");
            put("socketFamily", 10);
            put("socketFrom", "host2");
            put("socketFromPort", 10);
            put("socketTo", "host1");
            put("socketToPort", 10);
            put("_unmatchedSize", 5);
            put("_complete", false);
        }};

        HashMap<String, Object> rcv5 = new HashMap<String, Object>() {{
            put(Neo4jExecutionGraph.EVENT_ID_PROPERTY, "rcv-5");
            put("threadId", "9/9@host1");
            put("size", 5);
            put("kernelTime", 5);
            put("socketId", "socket-id");
            put("socketFamily", 10);
            put("socketFrom", "host2");
            put("socketFromPort", 10);
            put("socketTo", "host1");
            put("socketToPort", 10);
            put("_unmatchedSize", 5);
            put("_complete", false);
        }};

        HashMap<String, Object> snd1 = new HashMap<String, Object>() {{
            put(Neo4jExecutionGraph.EVENT_ID_PROPERTY, "snd-1");
            put("threadId", "9/9@host2");
            put("size", 1);
            put("kernelTime", 1);
            put("socketId", "socket-id");
            put("socketFamily", 10);
            put("socketFrom", "host2");
            put("socketFromPort", 10);
            put("socketTo", "host1");
            put("socketToPort", 10);
            put("_unmatchedSize", 0);
            put("_complete", true);
        }};

        HashMap<String, Object> snd2 = new HashMap<String, Object>() {{
            put(Neo4jExecutionGraph.EVENT_ID_PROPERTY, "snd-2");
            put("threadId", "9/9@host2");
            put("size", 6);
            put("kernelTime", 2);
            put("socketId", "socket-id");
            put("socketFamily", 10);
            put("socketFrom", "host2");
            put("socketFromPort", 10);
            put("socketTo", "host1");
            put("socketToPort", 10);
            put("_unmatchedSize", 0);
            put("_complete", true);
        }};

        graph.getSession().run(
                "CREATE (rcv1:RCV:EVENT)-[:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]->(rcv2:RCV:EVENT) " +
                        "CREATE (rcv2)-[:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]->(rcv3:RCV:EVENT) " +
                        "CREATE (rcv3)-[:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]->(rcv4:RCV:EVENT) " +
                        "CREATE (rcv4)-[:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]->(rcv5:RCV:EVENT) " +
                        "SET rcv1 = $rcv1 " +
                        "SET rcv2 = $rcv2 " +
                        "SET rcv3 = $rcv3 " +
                        "SET rcv4 = $rcv4 " +
                        "SET rcv5 = $rcv5 " +
                        "WITH rcv1, rcv2, rcv3 " +
                        "CREATE (snd1:SND:EVENT)-[:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]->(snd2:SND:EVENT) " +
                        "SET snd1 = $snd1 " +
                        "SET snd2 = $snd2 " +
                        "CREATE (snd1)-[snd1_rcv1:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]->(rcv1) " +
                        "CREATE (snd2)-[snd2_rcv2:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]->(rcv2) " +
                        "CREATE (snd2)-[snd2_rcv3:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]->(rcv3) ",
                new HashMap<String, Object>() {{
                    put("rcv1", rcv1);
                    put("rcv2", rcv2);
                    put("rcv3", rcv3);
                    put("rcv4", rcv4);
                    put("rcv5", rcv5);
                    put("snd1", snd1);
                    put("snd2", snd2);
                }}
        );

        SocketSend sendEvent = new SocketSend();
        sendEvent.setId("snd-3");
        sendEvent.setUserTime(1);
        sendEvent.setKernelTime(3);
        sendEvent.setPid(9);
        sendEvent.setTid(10);
        sendEvent.setComm("comm");
        sendEvent.setHost("host2");
        sendEvent.setSocketId("socket-id");
        sendEvent.setSocketFamily(10);
        sendEvent.setSocketFrom("host2");
        sendEvent.setSourcePort(10);
        sendEvent.setSocketTo("host1");
        sendEvent.setDestinationPort(10);
        sendEvent.setSize(12);
        graph.addEvent(sendEvent);

        // Expected: (snd-3)-->{ (rcv-2), (rcv-3), (rcv-4) };
        List<Record> records = graph.getSession().run(
                "MATCH (snd3:SND:EVENT)-[:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]->(rcv:RCV:EVENT) " +
                        "WHERE snd3.eventId = 'snd-3' " +
                        "RETURN rcv.eventId as rcvId"
        ).list();

        assertEquals(3, records.size());

        List<String> rcvIds = records.stream().map(record -> record.get("rcvId").asString()).collect(Collectors.toList());
        assertFalse(rcvIds.contains("rcv-1"));
        assertFalse(rcvIds.contains("rcv-2"));
        assertTrue(rcvIds.contains("rcv-3"));
        assertTrue(rcvIds.contains("rcv-4"));
        assertTrue(rcvIds.contains("rcv-5"));

        // Expected unmatched sizes of RCV and SND events
        Record record;
        assertEquals(0, (record = graph.getSession().run("MATCH (snd:SND:EVENT) WHERE snd.eventId = 'snd-3' RETURN snd._unmatchedSize as remainingSize, snd._complete as isComplete").single()).get("remainingSize").asInt());
        assertTrue(record.get("isComplete").asBoolean());
        assertEquals(0, (record = graph.getSession().run("MATCH (rcv:RCV:EVENT) WHERE rcv.eventId = 'rcv-3' RETURN rcv._unmatchedSize as remainingSize, rcv._complete as isComplete").single()).get("remainingSize").asInt());
        assertTrue(record.get("isComplete").asBoolean());
        assertEquals(0, (record = graph.getSession().run("MATCH (rcv:RCV:EVENT) WHERE rcv.eventId = 'rcv-4' RETURN rcv._unmatchedSize as remainingSize, rcv._complete as isComplete").single()).get("remainingSize").asInt());
        assertTrue(record.get("isComplete").asBoolean());
        assertEquals(2, (record = graph.getSession().run("MATCH (rcv:RCV:EVENT) WHERE rcv.eventId = 'rcv-5' RETURN rcv._unmatchedSize as remainingSize, rcv._complete as isComplete").single()).get("remainingSize").asInt());
        assertFalse(record.get("isComplete").asBoolean());
    }

    @Test
    void addSocketReceiveAppendsToExistingSendEvents() {
        HashMap<String, Object> snd1 = new HashMap<String, Object>() {{
            put(Neo4jExecutionGraph.EVENT_ID_PROPERTY, "snd-1");
            put("threadId", "9/9@host1");
            put("size", 1);
            put("kernelTime", 1);
            put("socketId", "socket-id");
            put("socketFamily", 10);
            put("socketFrom", "host1");
            put("socketFromPort", 10);
            put("socketTo", "host2");
            put("socketToPort", 10);
            put("_unmatchedSize", 0);
            put("_complete", true);
        }};

        HashMap<String, Object> snd2 = new HashMap<String, Object>() {{
            put(Neo4jExecutionGraph.EVENT_ID_PROPERTY, "snd-2");
            put("threadId", "9/9@host1");
            put("size", 5);
            put("kernelTime", 2);
            put("socketId", "socket-id");
            put("socketFamily", 10);
            put("socketFrom", "host1");
            put("socketFromPort", 10);
            put("socketTo", "host2");
            put("socketToPort", 10);
            put("_unmatchedSize", 0);
            put("_complete", true);
        }};

        HashMap<String, Object> snd3 = new HashMap<String, Object>() {{
            put(Neo4jExecutionGraph.EVENT_ID_PROPERTY, "snd-3");
            put("threadId", "9/9@host1");
            put("size", 5);
            put("kernelTime", 3);
            put("socketId", "socket-id");
            put("socketFamily", 10);
            put("socketFrom", "host1");
            put("socketFromPort", 10);
            put("socketTo", "host2");
            put("socketToPort", 10);
            put("_unmatchedSize", 4);
            put("_complete", false);
        }};

        HashMap<String, Object> snd4 = new HashMap<String, Object>() {{
            put(Neo4jExecutionGraph.EVENT_ID_PROPERTY, "snd-4");
            put("threadId", "9/9@host1");
            put("size", 5);
            put("kernelTime", 4);
            put("socketId", "socket-id");
            put("socketFamily", 10);
            put("socketFrom", "host1");
            put("socketFromPort", 10);
            put("socketTo", "host2");
            put("socketToPort", 10);
            put("_unmatchedSize", 5);
            put("_complete", false);
        }};

        HashMap<String, Object> snd5 = new HashMap<String, Object>() {{
            put(Neo4jExecutionGraph.EVENT_ID_PROPERTY, "snd-5");
            put("threadId", "9/9@host1");
            put("size", 5);
            put("kernelTime", 5);
            put("socketId", "socket-id");
            put("socketFamily", 10);
            put("socketFrom", "host1");
            put("socketFromPort", 10);
            put("socketTo", "host2");
            put("socketToPort", 10);
            put("_unmatchedSize", 5);
            put("_complete", false);
        }};

        HashMap<String, Object> rcv1 = new HashMap<String, Object>() {{
            put(Neo4jExecutionGraph.EVENT_ID_PROPERTY, "rcv-1");
            put("threadId", "9/9@host2");
            put("size", 1);
            put("kernelTime", 1);
            put("socketId", "socket-id");
            put("socketFamily", 10);
            put("socketFrom", "host1");
            put("socketFromPort", 10);
            put("socketTo", "host2");
            put("socketToPort", 10);
            put("_unmatchedSize", 0);
            put("_complete", true);
        }};

        HashMap<String, Object> rcv2 = new HashMap<String, Object>() {{
            put(Neo4jExecutionGraph.EVENT_ID_PROPERTY, "rcv-2");
            put("threadId", "9/9@host2");
            put("size", 6);
            put("kernelTime", 2);
            put("socketId", "socket-id");
            put("socketFamily", 10);
            put("socketFrom", "host1");
            put("socketFromPort", 10);
            put("socketTo", "host2");
            put("socketToPort", 10);
            put("_unmatchedSize", 0);
            put("_complete", true);
        }};

        graph.getSession().run(
                "CREATE (snd1:SND:EVENT)-[:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]->(snd2:SND:EVENT) " +
                        "CREATE (snd2)-[:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]->(snd3:SND:EVENT) " +
                        "CREATE (snd3)-[:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]->(snd4:SND:EVENT) " +
                        "CREATE (snd4)-[:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]->(snd5:SND:EVENT) " +
                        "SET snd1 = $snd1 " +
                        "SET snd2 = $snd2 " +
                        "SET snd3 = $snd3 " +
                        "SET snd4 = $snd4 " +
                        "SET snd5 = $snd5 " +
                        "WITH snd1, snd2, snd3 " +
                        "CREATE (rcv1:RCV:EVENT)-[:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]->(rcv2:RCV:EVENT) " +
                        "SET rcv1 = $rcv1 " +
                        "SET rcv2 = $rcv2 " +
                        "CREATE (snd1)-[snd1_rcv1:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]->(rcv1) " +
                        "CREATE (snd2)-[snd2_rcv2:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]->(rcv2) " +
                        "CREATE (snd3)-[snd3_rcv2:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]->(rcv2) ",
                new HashMap<String, Object>() {{
                    put("snd1", snd1);
                    put("snd2", snd2);
                    put("snd3", snd3);
                    put("snd4", snd4);
                    put("snd5", snd5);
                    put("rcv1", rcv1);
                    put("rcv2", rcv2);
                }}
        );

        SocketReceive receiveEvent = new SocketReceive();
        receiveEvent.setId("rcv-3");
        receiveEvent.setUserTime(1);
        receiveEvent.setKernelTime(3);
        receiveEvent.setPid(9);
        receiveEvent.setTid(10);
        receiveEvent.setComm("comm");
        receiveEvent.setHost("host2");
        receiveEvent.setSocketId("socket-id");
        receiveEvent.setSocketFamily(10);
        receiveEvent.setSocketFrom("host1");
        receiveEvent.setSourcePort(10);
        receiveEvent.setSocketTo("host2");
        receiveEvent.setDestinationPort(10);
        receiveEvent.setSize(12);
        graph.addEvent(receiveEvent);

        // Expected: { (snd-2), (snd-3), (snd-4) } --> (rcv3);
        List<Record> records = graph.getSession().run(
                "MATCH (snd:SND:EVENT)-[:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]->(rcv3:RCV:EVENT) " +
                        "WHERE rcv3.eventId = 'rcv-3' " +
                        "RETURN snd.eventId as sndId"
        ).list();

        assertEquals(3, records.size());

        List<String> sndIds = records.stream().map(record -> record.get("sndId").asString()).collect(Collectors.toList());
        assertFalse(sndIds.contains("snd-1"));
        assertFalse(sndIds.contains("snd-2"));
        assertTrue(sndIds.contains("snd-3"));
        assertTrue(sndIds.contains("snd-4"));
        assertTrue(sndIds.contains("snd-5"));

        // Expected unmatched sizes of RCV and SND events
        Record record;
        assertEquals(0, (record = graph.getSession().run("MATCH (rcv:RCV:EVENT) WHERE rcv.eventId = 'rcv-3' RETURN rcv._unmatchedSize as remainingSize, rcv._complete as isComplete").single()).get("remainingSize").asInt());
        assertTrue(record.get("isComplete").asBoolean());
        assertEquals(0, (record = graph.getSession().run("MATCH (snd:SND:EVENT) WHERE snd.eventId = 'snd-3' RETURN snd._unmatchedSize as remainingSize, snd._complete as isComplete").single()).get("remainingSize").asInt());
        assertTrue(record.get("isComplete").asBoolean());
        assertEquals(0, (record = graph.getSession().run("MATCH (snd:SND:EVENT) WHERE snd.eventId = 'snd-4' RETURN snd._unmatchedSize as remainingSize, snd._complete as isComplete").single()).get("remainingSize").asInt());
        assertTrue(record.get("isComplete").asBoolean());
        assertEquals(2, (record = graph.getSession().run("MATCH (snd:SND:EVENT) WHERE snd.eventId = 'snd-5' RETURN snd._unmatchedSize as remainingSize, snd._complete as isComplete").single()).get("remainingSize").asInt());
    }

    @Test
    void addEventWithCustomExtraData() {
        JsonObject extraData = new JsonObject();

        extraData.add("custom_string_property", new JsonPrimitive("value"));
        extraData.add("custom_number_property", new JsonPrimitive(1));

        ProcessCreate create = new ProcessCreate();
        create.setId("create");
        create.setUserTime(1);
        create.setKernelTime(1);
        create.setPid(9);
        create.setTid(9);
        create.setComm("comm");
        create.setHost("host");
        create.setChildPid(10);
        create.setExtraData(extraData);
        graph.addEvent(create);

        assertTrue(graph.getSession().run(
                "MATCH (n) " +
                        "WHERE n.eventId = $eventId AND n.custom_string_property = $val1 AND n.custom_number_property = $val2 " +
                        "RETURN n",
                new HashMap<String, Object>() {{
                    put("eventId", create.getId());
                    put("val1", "value");
                    put("val2", "1");
                }}
        ).hasNext());

        ProcessStart start = new ProcessStart();
        start.setId("start");
        start.setUserTime(1);
        start.setKernelTime(2);
        start.setPid(9);
        start.setTid(10);
        start.setComm("comm");
        start.setHost("host");
        start.setExtraData(extraData);
        graph.addEvent(start);

        assertTrue(graph.getSession().run(
                "MATCH (n) " +
                        "WHERE n.eventId = $eventId AND n.custom_string_property = $val1 AND n.custom_number_property = $val2 " +
                        "RETURN n",
                new HashMap<String, Object>() {{
                    put("eventId", start.getId());
                    put("val1", "value");
                    put("val2", "1");
                }}
        ).hasNext());

        ProcessEnd end = new ProcessEnd();
        end.setId("end");
        end.setUserTime(1);
        end.setKernelTime(1);
        end.setPid(9);
        end.setTid(10);
        end.setComm("comm");
        end.setHost("host");
        end.setExtraData(extraData);
        graph.addEvent(end);

        assertTrue(graph.getSession().run(
                "MATCH (n) " +
                        "WHERE n.eventId = $eventId AND n.custom_string_property = $val1 AND n.custom_number_property = $val2 " +
                        "RETURN n",
                new HashMap<String, Object>() {{
                    put("eventId", end.getId());
                    put("val1", "value");
                    put("val2", "1");
                }}
        ).hasNext());

        ProcessJoin join = new ProcessJoin();
        join.setId("join");
        join.setUserTime(1);
        join.setKernelTime(2);
        join.setPid(9);
        join.setTid(9);
        join.setComm("comm");
        join.setHost("host");
        join.setChildPid(10);
        join.setExtraData(extraData);
        graph.addEvent(join);

        assertTrue(graph.getSession().run(
                "MATCH (n) " +
                        "WHERE n.eventId = $eventId AND n.custom_string_property = $val1 AND n.custom_number_property = $val2 " +
                        "RETURN n",
                new HashMap<String, Object>() {{
                    put("eventId", join.getId());
                    put("val1", "value");
                    put("val2", "1");
                }}
        ).hasNext());

        SocketAccept accept = new SocketAccept();
        accept.setId("accept");
        accept.setUserTime(1);
        accept.setKernelTime(2);
        accept.setPid(9);
        accept.setTid(9);
        accept.setComm("comm");
        accept.setHost("localhost");
        accept.setSocketId("socket-test-id");
        accept.setSocketFamily(10);
        accept.setSocketFrom("0.0.0.0");
        accept.setSourcePort(10);
        accept.setSocketTo("0.0.0.0");
        accept.setDestinationPort(10);
        accept.setExtraData(extraData);
        graph.addEvent(accept);

        assertTrue(graph.getSession().run(
                "MATCH (n) " +
                        "WHERE n.eventId = $eventId AND n.custom_string_property = $val1 AND n.custom_number_property = $val2 " +
                        "RETURN n",
                new HashMap<String, Object>() {{
                    put("eventId", accept.getId());
                    put("val1", "value");
                    put("val2", "1");
                }}
        ).hasNext());

        SocketConnect connect = new SocketConnect();
        connect.setId("connect");
        connect.setUserTime(1);
        connect.setKernelTime(2);
        connect.setPid(9);
        connect.setTid(10);
        connect.setComm("comm");
        connect.setHost("localhost");
        connect.setSocketId("socket-test-id");
        connect.setSocketFamily(10);
        connect.setSocketFrom("0.0.0.0");
        connect.setSourcePort(10);
        connect.setSocketTo("0.0.0.0");
        connect.setDestinationPort(10);
        connect.setExtraData(extraData);
        graph.addEvent(connect);

        assertTrue(graph.getSession().run(
                "MATCH (n) " +
                        "WHERE n.eventId = $eventId AND n.custom_string_property = $val1 AND n.custom_number_property = $val2 " +
                        "RETURN n",
                new HashMap<String, Object>() {{
                    put("eventId", connect.getId());
                    put("val1", "value");
                    put("val2", "1");
                }}
        ).hasNext());

        SocketReceive receive = new SocketReceive();
        receive.setId("rcv");
        receive.setUserTime(1);
        receive.setKernelTime(4);
        receive.setPid(9);
        receive.setTid(10);
        receive.setComm("comm");
        receive.setHost("host2");
        receive.setSocketId("socket-id");
        receive.setSocketFamily(10);
        receive.setSocketFrom("0.0.0.0");
        receive.setSourcePort(10);
        receive.setSocketTo("0.0.0.0");
        receive.setDestinationPort(10);
        receive.setSize(2);
        receive.setExtraData(extraData);
        graph.addEvent(receive);

        assertTrue(graph.getSession().run(
                "MATCH (n) " +
                        "WHERE n.eventId = $eventId AND n.custom_string_property = $val1 AND n.custom_number_property = $val2 " +
                        "RETURN n",
                new HashMap<String, Object>() {{
                    put("eventId", receive.getId());
                    put("val1", "value");
                    put("val2", "1");
                }}
        ).hasNext());

        SocketSend send = new SocketSend();
        send.setId("snd");
        send.setUserTime(1);
        send.setKernelTime(3);
        send.setPid(9);
        send.setTid(10);
        send.setComm("comm");
        send.setHost("host2");
        send.setSocketId("socket-id");
        send.setSocketFamily(10);
        send.setSocketFrom("0.0.0.0");
        send.setSourcePort(10);
        send.setSocketTo("0.0.0.0");
        send.setDestinationPort(10);
        send.setSize(2);
        send.setExtraData(extraData);
        graph.addEvent(send);

        assertTrue(graph.getSession().run(
                "MATCH (n) " +
                        "WHERE n.eventId = $eventId AND n.custom_string_property = $val1 AND n.custom_number_property = $val2 " +
                        "RETURN n",
                new HashMap<String, Object>() {{
                    put("eventId", send.getId());
                    put("val1", "value");
                    put("val2", "1");
                }}
        ).hasNext());

        Log log = new Log();
        log.setId("log");
        log.setUserTime(1);
        log.setKernelTime(3);
        log.setPid(9);
        log.setTid(10);
        log.setComm("comm");
        log.setHost("host2");
        log.setExtraData(extraData);
        graph.addEvent(log);

        assertTrue(graph.getSession().run(
                "MATCH (n) " +
                        "WHERE n.eventId = $eventId AND n.custom_string_property = $val1 AND n.custom_number_property = $val2 " +
                        "RETURN n",
                new HashMap<String, Object>() {{
                    put("eventId", log.getId());
                    put("val1", "value");
                    put("val2", "1");
                }}
        ).hasNext());
    }

    @Test
    void addLogEventBetweenTwoExistingEvents() {
        JsonObject extraData = new JsonObject();
        extraData.add("message", new JsonPrimitive("creating new thread"));

        Log log = new Log();
        log.setId("log1");
        log.setUserTime(1);
        log.setKernelTime(2);
        log.setPid(9);
        log.setTid(9);
        log.setComm("comm");
        log.setHost("host");
        log.setExtraData(extraData);
        graph.addEvent(log);

        log = new Log();
        log.setId("log3");
        log.setUserTime(1);
        log.setKernelTime(4);
        log.setPid(9);
        log.setTid(9);
        log.setComm("comm");
        log.setHost("host");
        log.setExtraData(extraData);
        graph.addEvent(log);

        log = new Log();
        log.setId("log2");
        log.setUserTime(1);
        log.setKernelTime(3);
        log.setPid(9);
        log.setTid(9);
        log.setComm("comm");
        log.setHost("host");
        log.setExtraData(extraData);
        graph.addEvent(log);

        try {
            Record record = graph.getSession().run(
                    "MATCH (log1:EVENT)-[:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]-(log2:EVENT)-[:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]->(log3:EVENT) " +
                            "WHERE log1.eventId = 'log1' AND log2.eventId = 'log2' AND log3.eventId = 'log3' " +
                            "RETURN 'OK'"
            ).single();

            assertEquals("OK", record.get(0).asString());
        } catch (NoSuchElementException e) {
            fail("Couldn't find the expected path.");
        }
    }

    @Test
    public void addSndAndRcvEventsWithBidirectionalConnection() {

        // Prepare scenario:
        // host1 sends message to host2 (snd-1)
        // host2 receives message from host2 (rcv-1)
        // host2 sends message to host1 (snd-2)
        // host1 receives message from host1 (rcv-2)

        SocketSend snd1 = new SocketSend();
        snd1.setId("snd-1");
        snd1.setUserTime(1);
        snd1.setKernelTime(50);
        snd1.setPid(10);
        snd1.setTid(10);
        snd1.setComm("comm");
        snd1.setHost("host");
        snd1.setSocketId("socket-id");
        snd1.setSocketFamily(10);
        snd1.setSocketFrom("0.0.0.0");
        snd1.setSourcePort(10);
        snd1.setSocketTo("0.0.0.0");
        snd1.setDestinationPort(10);
        snd1.setSize(10);
        graph.addEvent(snd1);

        SocketReceive rcv1 = new SocketReceive();
        rcv1.setId("rcv-1");
        rcv1.setUserTime(1);
        rcv1.setKernelTime(5);
        rcv1.setPid(10);
        rcv1.setTid(10);
        rcv1.setComm("comm");
        rcv1.setHost("host2");
        rcv1.setSocketId("socket-id");
        rcv1.setSocketFamily(10);
        rcv1.setSocketFrom("0.0.0.0");
        rcv1.setSourcePort(10);
        rcv1.setSocketTo("0.0.0.0");
        rcv1.setDestinationPort(10);
        rcv1.setSize(10);
        graph.addEvent(rcv1);

        SocketSend snd2 = new SocketSend();
        snd2.setId("snd-2");
        snd2.setUserTime(1);
        snd2.setKernelTime(10);
        snd2.setPid(10);
        snd2.setTid(10);
        snd2.setComm("comm");
        snd2.setHost("host2");
        snd2.setSocketId("socket-id");
        snd2.setSocketFamily(10);
        snd2.setSocketFrom("0.0.0.0");
        snd2.setSourcePort(10);
        snd2.setSocketTo("0.0.0.0");
        snd2.setDestinationPort(10);
        snd2.setSize(10);
        graph.addEvent(snd2);

        SocketReceive rcv2 = new SocketReceive();
        rcv2.setId("rcv-2");
        rcv2.setUserTime(1);
        rcv2.setKernelTime(60);
        rcv2.setPid(10);
        rcv2.setTid(10);
        rcv2.setComm("comm");
        rcv2.setHost("host");
        rcv2.setSocketId("socket-id");
        rcv2.setSocketFamily(10);
        rcv2.setSocketFrom("0.0.0.0");
        rcv2.setSourcePort(10);
        rcv2.setSocketTo("0.0.0.0");
        rcv2.setDestinationPort(10);
        rcv2.setSize(10);
        graph.addEvent(rcv2);


        // Expected:
        //
        // host1: (snd-1)->(rcv-2)
        // host2: (rcv-1)->(snd-2)
        // HAPPENS_BEFORE: (snd-1)->(rcv-1); (snd-2)->(rcv-2)
        try {
            Record record = graph.getSession().run(
                    "MATCH (snd1:EVENT)-[:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]->(rcv2:EVENT) " +
                            "WHERE snd1.eventId = 'snd-1' AND rcv2.eventId = 'rcv-2' " +

                            "MATCH (rcv1:EVENT)-[:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]->(snd2:EVENT) " +
                            "WHERE rcv1.eventId = 'rcv-1' AND snd2.eventId = 'snd-2' " +

                            "MATCH (snd1:EVENT)-[:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]->(rcv1:EVENT) " +
                            "WHERE snd1.eventId = 'snd-1' AND rcv1.eventId = 'rcv-1' " +

                            "MATCH (snd2:EVENT)-[:" + Neo4jExecutionGraph.HAPPENS_BEFORE_LABEL + "]->(rcv2:EVENT) " +
                            "WHERE snd1.eventId = 'snd-1' AND rcv2.eventId = 'rcv-2' " +

                            "RETURN 'OK'"
            ).single();

            assertEquals("OK", record.get(0).asString());
        } catch (NoSuchElementException e) {
            fail("Couldn't find the expected path.");
        }

        // Expected unmatched sizes of RCV and SND events
        Record record;
        assertTrue((record = graph.getSession().run("MATCH (snd:SND:EVENT) WHERE snd.eventId = 'snd-1' RETURN snd._unmatchedSize = 0 as remainingSize, snd._complete as isComplete").single()).get("remainingSize", false));
        assertTrue(record.get("isComplete", true));
        assertTrue((record = graph.getSession().run("MATCH (rcv:RCV:EVENT) WHERE rcv.eventId = 'rcv-1' RETURN rcv._unmatchedSize = 0 as remainingSize").single()).get("remainingSize", false));
        assertTrue(record.get("isComplete", true));
        assertTrue((record = graph.getSession().run("MATCH (snd:SND:EVENT) WHERE snd.eventId = 'snd-2' RETURN snd._unmatchedSize = 0 as remainingSize").single()).get("remainingSize", false));
        assertTrue(record.get("isComplete", true));
        assertTrue((record = graph.getSession().run("MATCH (rcv:RCV:EVENT) WHERE rcv.eventId = 'rcv-2' RETURN rcv._unmatchedSize = 0 as remainingSize").single()).get("remainingSize", false));
        assertTrue(record.get("isComplete", true));
    }

}
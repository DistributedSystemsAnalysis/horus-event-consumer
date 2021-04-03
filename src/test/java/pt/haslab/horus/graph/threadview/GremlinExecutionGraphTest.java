package pt.haslab.horus.graph.threadview;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import pt.haslab.horus.BaseTest;
import pt.haslab.horus.Config;
import pt.haslab.horus.events.*;
import pt.haslab.horus.graph.datastores.gremlin.GremlinExecutionGraph;

import static org.junit.jupiter.api.Assertions.*;

class GremlinExecutionGraphTest
        extends BaseTest {
    private static GremlinExecutionGraph graph;

    @BeforeAll
    static void openGraph() {
        Configuration config = Config.getInstance().getConfig();
        config.setProperty("gremlin.graph", "org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph");
        config.setProperty("gremlin.neo4j.directory", "/tmp/_horus-neo4j");

        Graph localGraph = org.apache.tinkerpop.gremlin.structure.util.GraphFactory.open(
                Config.getInstance().getConfig());

        graph = new GremlinExecutionGraph(localGraph);
    }

    @AfterAll
    static void closeGraph() {
        try {
            graph.close();
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

        Graph g = graph.getGraph();

        assertTrue(g.traversal().V().has(GremlinExecutionGraph.EVENT_ID, create1.getId()).out(
                GremlinExecutionGraph.HAPPENS_BEFORE_LABEL).has(GremlinExecutionGraph.EVENT_ID,
                create2.getId()).out(
                GremlinExecutionGraph.HAPPENS_BEFORE_LABEL).has(GremlinExecutionGraph.EVENT_ID,
                create3.getId()).hasNext());
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

        // Expected: (create-2) --happens-before-> (create-3)
        Graph g = graph.getGraph();
        assertTrue(g.traversal().V().has(GremlinExecutionGraph.EVENT_ID, create2.getId()).out(
                GremlinExecutionGraph.HAPPENS_BEFORE_LABEL).has(GremlinExecutionGraph.EVENT_ID,
                create3.getId()).hasNext());
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

        Graph g = graph.getGraph();
        Vertex actualStart = g.traversal().V().has(GremlinExecutionGraph.EVENT_ID, createEvent.getId()).out(
                GremlinExecutionGraph.HAPPENS_BEFORE_LABEL).next();

        assertEquals(startEvent.getId(), actualStart.property(GremlinExecutionGraph.EVENT_ID).value());
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

        Graph g = graph.getGraph();
        Vertex actualStart = g.traversal().V().has(GremlinExecutionGraph.EVENT_ID, createEvent.getId()).out(
                GremlinExecutionGraph.HAPPENS_BEFORE_LABEL).next();

        assertEquals(startEvent.getId(), actualStart.property(GremlinExecutionGraph.EVENT_ID).value());
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

        Graph g = graph.getGraph();
        Vertex actualJoin = g.traversal().V().has(GremlinExecutionGraph.EVENT_ID, endEvent.getId()).out(
                GremlinExecutionGraph.HAPPENS_BEFORE_LABEL).next();

        assertEquals(joinEvent.getId(), actualJoin.property(GremlinExecutionGraph.EVENT_ID).value());
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

        Graph g = graph.getGraph();
        Vertex actualJoin = g.traversal().V().has(GremlinExecutionGraph.EVENT_ID, endEvent.getId()).out(
                GremlinExecutionGraph.HAPPENS_BEFORE_LABEL).next();

        assertEquals(joinEvent.getId(), actualJoin.property(GremlinExecutionGraph.EVENT_ID).value());
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

        Graph g = graph.getGraph();
        Vertex actualAccept = g.traversal().V().has(GremlinExecutionGraph.EVENT_ID, connectEvent.getId()).out(
                GremlinExecutionGraph.HAPPENS_BEFORE_LABEL).next();

        assertEquals(acceptEvent.getId(), actualAccept.property(GremlinExecutionGraph.EVENT_ID).value());
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

        Graph g = graph.getGraph();
        Vertex actualAccept = g.traversal().V().has(GremlinExecutionGraph.EVENT_ID, connectEvent.getId()).out(
                GremlinExecutionGraph.HAPPENS_BEFORE_LABEL).next();

        assertEquals(acceptEvent.getId(), actualAccept.property(GremlinExecutionGraph.EVENT_ID).value());
    }

    @Test
    void addSocketSendAppendsToExistingReceiveEvents() {
        Graph g = graph.getGraph();

        Vertex rcv1 = g.addVertex(T.label, EventType.RCV.toString(), GremlinExecutionGraph.EVENT_ID, "rcv-1", "threadId",
                "9/9@host1", "size", 1,
                "kernelTime", 1, "socketId", "socket-id", "socketFamily", 10);
        Vertex rcv2 = g.addVertex(T.label, EventType.RCV.toString(), GremlinExecutionGraph.EVENT_ID, "rcv-2", "threadId",
                "9/9@host1", "size", 5,
                "kernelTime", 2, "socketId", "socket-id", "socketFamily", 10);
        Vertex rcv3 = g.addVertex(T.label, EventType.RCV.toString(), GremlinExecutionGraph.EVENT_ID, "rcv-3", "threadId",
                "9/9@host1", "size", 5,
                "kernelTime", 3, "socketId", "socket-id", "socketFamily", 10);
        Vertex rcv4 = g.addVertex(T.label, EventType.RCV.toString(), GremlinExecutionGraph.EVENT_ID, "rcv-4", "threadId",
                "9/9@host1", "size", 5,
                "kernelTime", 4, "socketId", "socket-id", "socketFamily", 10);
        Vertex rcv5 = g.addVertex(T.label, EventType.RCV.toString(), GremlinExecutionGraph.EVENT_ID, "rcv-5", "threadId",
                "9/9@host1", "size", 5,
                "kernelTime", 5, "socketId", "socket-id", "socketFamily", 10);
        Vertex snd1 = g.addVertex(T.label, EventType.SND.toString(), GremlinExecutionGraph.EVENT_ID, "snd-1", "threadId",
                "9/10@host2", "size", 1,
                "kernelTime", 1, "socketId", "socket-id", "socketFamily", 10);
        Vertex snd2 = g.addVertex(T.label, EventType.SND.toString(), GremlinExecutionGraph.EVENT_ID, "snd-2", "threadId",
                "9/10@host2", "size", 1,
                "kernelTime", 2, "socketId", "socket-id", "socketFamily", 10);

        rcv1.addEdge(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL, rcv2);
        rcv2.addEdge(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL, rcv3);
        rcv3.addEdge(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL, rcv4);
        rcv4.addEdge(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL, rcv5);
        snd1.addEdge(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL, rcv1, "size", 1);
        snd2.addEdge(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL, rcv2, "size", 1);

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
        sendEvent.setSocketFrom("0.0.0.0");
        sendEvent.setSourcePort(10);
        sendEvent.setSocketTo("0.0.0.0");
        sendEvent.setDestinationPort(10);
        sendEvent.setSize(12);
        graph.addEvent(sendEvent);

        assertFalse(g.traversal().V().has(GremlinExecutionGraph.EVENT_ID, sendEvent.getId()).out(
                GremlinExecutionGraph.HAPPENS_BEFORE_LABEL).has(GremlinExecutionGraph.EVENT_ID,
                rcv1.property(GremlinExecutionGraph.EVENT_ID).value()).hasNext());
        assertTrue(g.traversal().V().has(GremlinExecutionGraph.EVENT_ID, sendEvent.getId()).out(
                GremlinExecutionGraph.HAPPENS_BEFORE_LABEL).has(GremlinExecutionGraph.EVENT_ID,
                rcv2.property(GremlinExecutionGraph.EVENT_ID).value()).hasNext());
        assertTrue(g.traversal().V().has(GremlinExecutionGraph.EVENT_ID, sendEvent.getId()).out(
                GremlinExecutionGraph.HAPPENS_BEFORE_LABEL).has(GremlinExecutionGraph.EVENT_ID,
                rcv3.property(GremlinExecutionGraph.EVENT_ID).value()).hasNext());
        assertTrue(g.traversal().V().has(GremlinExecutionGraph.EVENT_ID, sendEvent.getId()).out(
                GremlinExecutionGraph.HAPPENS_BEFORE_LABEL).has(GremlinExecutionGraph.EVENT_ID,
                rcv4.property(GremlinExecutionGraph.EVENT_ID).value()).hasNext());
        assertFalse(g.traversal().V().has(GremlinExecutionGraph.EVENT_ID, sendEvent.getId()).out(
                GremlinExecutionGraph.HAPPENS_BEFORE_LABEL).has(GremlinExecutionGraph.EVENT_ID,
                rcv5.property(GremlinExecutionGraph.EVENT_ID).value()).hasNext());
    }

    @Test
    void addSocketReceiveAppendsToExistingSendEvents() {
        Graph g = graph.getGraph();

        Vertex snd1 = g.addVertex(T.label, EventType.SND.toString(), GremlinExecutionGraph.EVENT_ID, "snd-1", "threadId",
                "9/9@host1", "size", 1,
                "kernelTime", 1, "socketId", "socket-id", "socketFamily", 10);
        Vertex snd2 = g.addVertex(T.label, EventType.SND.toString(), GremlinExecutionGraph.EVENT_ID, "snd-2", "threadId",
                "9/9@host1", "size", 5,
                "kernelTime", 2, "socketId", "socket-id", "socketFamily", 10);
        Vertex snd3 = g.addVertex(T.label, EventType.SND.toString(), GremlinExecutionGraph.EVENT_ID, "snd-3", "threadId",
                "9/9@host1", "size", 5,
                "kernelTime", 3, "socketId", "socket-id", "socketFamily", 10);
        Vertex snd4 = g.addVertex(T.label, EventType.SND.toString(), GremlinExecutionGraph.EVENT_ID, "snd-4", "threadId",
                "9/9@host1", "size", 5,
                "kernelTime", 4, "socketId", "socket-id", "socketFamily", 10);
        Vertex snd5 = g.addVertex(T.label, EventType.SND.toString(), GremlinExecutionGraph.EVENT_ID, "snd-5", "threadId",
                "9/9@host1", "size", 5,
                "kernelTime", 5, "socketId", "socket-id", "socketFamily", 10);
        Vertex rcv1 = g.addVertex(T.label, EventType.RCV.toString(), GremlinExecutionGraph.EVENT_ID, "rcv-1", "threadId",
                "9/10@host2", "size", 1,
                "kernelTime", 1, "socketId", "socket-id", "socketFamily", 10);
        Vertex rcv2 = g.addVertex(T.label, EventType.RCV.toString(), GremlinExecutionGraph.EVENT_ID, "rcv-2", "threadId",
                "9/10@host2", "size", 1,
                "kernelTime", 2, "socketId", "socket-id", "socketFamily", 10);

        snd1.addEdge(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL, snd2);
        snd2.addEdge(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL, snd3);
        snd3.addEdge(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL, snd4);
        snd4.addEdge(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL, snd5);
        snd1.addEdge(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL, rcv1, "size", 1);
        snd2.addEdge(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL, rcv2, "size", 1);

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
        receiveEvent.setSocketFrom("0.0.0.0");
        receiveEvent.setSourcePort(10);
        receiveEvent.setSocketTo("0.0.0.0");
        receiveEvent.setDestinationPort(10);
        receiveEvent.setSize(12);
        graph.addEvent(receiveEvent);

        assertFalse(g.traversal().V().has(GremlinExecutionGraph.EVENT_ID, receiveEvent.getId()).in(
                GremlinExecutionGraph.HAPPENS_BEFORE_LABEL).has(GremlinExecutionGraph.EVENT_ID,
                snd1.property(GremlinExecutionGraph.EVENT_ID).value()).hasNext());

        assertTrue(g.traversal().V().has(GremlinExecutionGraph.EVENT_ID, receiveEvent.getId()).in(
                GremlinExecutionGraph.HAPPENS_BEFORE_LABEL).has(GremlinExecutionGraph.EVENT_ID,
                snd2.property(GremlinExecutionGraph.EVENT_ID).value()).hasNext());
        assertTrue(g.traversal().V().has(GremlinExecutionGraph.EVENT_ID, receiveEvent.getId()).in(
                GremlinExecutionGraph.HAPPENS_BEFORE_LABEL).has(GremlinExecutionGraph.EVENT_ID,
                snd3.property(GremlinExecutionGraph.EVENT_ID).value()).hasNext());
        assertTrue(g.traversal().V().has(GremlinExecutionGraph.EVENT_ID, receiveEvent.getId()).in(
                GremlinExecutionGraph.HAPPENS_BEFORE_LABEL).has(GremlinExecutionGraph.EVENT_ID,
                snd4.property(GremlinExecutionGraph.EVENT_ID).value()).hasNext());
        assertFalse(g.traversal().V().has(GremlinExecutionGraph.EVENT_ID, receiveEvent.getId()).in(
                GremlinExecutionGraph.HAPPENS_BEFORE_LABEL).has(GremlinExecutionGraph.EVENT_ID,
                snd5.property(GremlinExecutionGraph.EVENT_ID).value()).hasNext());
    }

    @Test
    void addSocketReceiveAppendsToExistingSendEventsWhichSizeIsNotFulfilled() {
        Graph g = graph.getGraph();

        Vertex snd1 = g.addVertex(T.label, EventType.SND.toString(), GremlinExecutionGraph.EVENT_ID, "snd-1", "threadId",
                "9/9@host1", "size", 2,
                "kernelTime", 1, "socketId", "socket-id", "socketFamily", 10);
        Vertex snd2 = g.addVertex(T.label, EventType.SND.toString(), GremlinExecutionGraph.EVENT_ID, "snd-2", "threadId",
                "9/9@host1", "size", 2,
                "kernelTime", 2, "socketId", "socket-id", "socketFamily", 10);
        Vertex rcv1 = g.addVertex(T.label, EventType.RCV.toString(), GremlinExecutionGraph.EVENT_ID, "rcv-1", "threadId",
                "9/10@host2", "size", 3,
                "kernelTime", 1, "socketId", "socket-id", "socketFamily", 10);

        snd1.addEdge(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL, snd2);
        snd1.addEdge(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL, rcv1, "size", 2);
        snd2.addEdge(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL, rcv1, "size", 1);

        SocketReceive receiveEvent = new SocketReceive();
        receiveEvent.setId("rcv-2");
        receiveEvent.setUserTime(1);
        receiveEvent.setKernelTime(4);
        receiveEvent.setPid(9);
        receiveEvent.setTid(10);
        receiveEvent.setComm("comm");
        receiveEvent.setHost("host2");
        receiveEvent.setSocketId("socket-id");
        receiveEvent.setSocketFamily(10);
        receiveEvent.setSocketFrom("0.0.0.0");
        receiveEvent.setSourcePort(10);
        receiveEvent.setSocketTo("0.0.0.0");
        receiveEvent.setDestinationPort(10);
        receiveEvent.setSize(2);
        graph.addEvent(receiveEvent);

        assertFalse(g.traversal().V().has(GremlinExecutionGraph.EVENT_ID, receiveEvent.getId()).in(
                GremlinExecutionGraph.HAPPENS_BEFORE_LABEL).has(GremlinExecutionGraph.EVENT_ID,
                snd1.property(GremlinExecutionGraph.EVENT_ID).value()).hasNext());
        assertTrue(g.traversal().V().has(GremlinExecutionGraph.EVENT_ID, receiveEvent.getId()).in(
                GremlinExecutionGraph.HAPPENS_BEFORE_LABEL).has(GremlinExecutionGraph.EVENT_ID,
                snd2.property(GremlinExecutionGraph.EVENT_ID).value()).hasNext());
    }

    @Test
    void addSocketSendAppendsToExistingSendEventsWhichSizeIsNotFulfilled() {
        Graph g = graph.getGraph();

        Vertex rcv1 = g.addVertex(T.label, EventType.RCV.toString(), GremlinExecutionGraph.EVENT_ID, "rcv-1", "threadId",
                "9/9@host1", "size", 2,
                "kernelTime", 1, "socketId", "socket-id", "socketFamily", 10);
        Vertex rcv2 = g.addVertex(T.label, EventType.RCV.toString(), GremlinExecutionGraph.EVENT_ID, "rcv-2", "threadId",
                "9/9@host1", "size", 2,
                "kernelTime", 2, "socketId", "socket-id", "socketFamily", 10);
        Vertex snd1 = g.addVertex(T.label, EventType.SND.toString(), GremlinExecutionGraph.EVENT_ID, "snd-1", "threadId",
                "9/10@host2", "size", 3,
                "kernelTime", 1, "socketId", "socket-id", "socketFamily", 10);

        rcv1.addEdge(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL, rcv2);
        snd1.addEdge(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL, rcv1, "size", 2);
        snd1.addEdge(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL, rcv2, "size", 1);

        SocketSend sendEvent = new SocketSend();
        sendEvent.setId("snd-2");
        sendEvent.setUserTime(1);
        sendEvent.setKernelTime(3);
        sendEvent.setPid(9);
        sendEvent.setTid(10);
        sendEvent.setComm("comm");
        sendEvent.setHost("host2");
        sendEvent.setSocketId("socket-id");
        sendEvent.setSocketFamily(10);
        sendEvent.setSocketFrom("0.0.0.0");
        sendEvent.setSourcePort(10);
        sendEvent.setSocketTo("0.0.0.0");
        sendEvent.setDestinationPort(10);
        sendEvent.setSize(2);
        graph.addEvent(sendEvent);

        assertFalse(g.traversal().V().has(GremlinExecutionGraph.EVENT_ID, sendEvent.getId()).out(
                GremlinExecutionGraph.HAPPENS_BEFORE_LABEL).has(GremlinExecutionGraph.EVENT_ID,
                rcv1.property(GremlinExecutionGraph.EVENT_ID).value()).hasNext());
        assertTrue(g.traversal().V().has(GremlinExecutionGraph.EVENT_ID, sendEvent.getId()).out(
                GremlinExecutionGraph.HAPPENS_BEFORE_LABEL).has(GremlinExecutionGraph.EVENT_ID,
                rcv2.property(GremlinExecutionGraph.EVENT_ID).value()).hasNext());
    }

    @Test
    void addEventWithCustomExtraData() {
        Graph g = graph.getGraph();
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

        assertTrue(g.traversal().V()
                .has(GremlinExecutionGraph.EVENT_ID, create.getId())
                .has("custom_string_property", "value")
                .has("custom_number_property", "1")
                .hasNext()
        );

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

        assertTrue(g.traversal().V()
                .has(GremlinExecutionGraph.EVENT_ID, start.getId())
                .has("custom_string_property", "value")
                .has("custom_number_property", "1")
                .hasNext()
        );

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

        assertTrue(g.traversal().V()
                .has(GremlinExecutionGraph.EVENT_ID, end.getId())
                .has("custom_string_property", "value")
                .has("custom_number_property", "1")
                .hasNext()
        );

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

        assertTrue(g.traversal().V()
                .has(GremlinExecutionGraph.EVENT_ID, join.getId())
                .has("custom_string_property", "value")
                .has("custom_number_property", "1")
                .hasNext()
        );

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

        assertTrue(g.traversal().V()
                .has(GremlinExecutionGraph.EVENT_ID, accept.getId())
                .has("custom_string_property", "value")
                .has("custom_number_property", "1")
                .hasNext()
        );

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

        assertTrue(g.traversal().V()
                .has(GremlinExecutionGraph.EVENT_ID, connect.getId())
                .has("custom_string_property", "value")
                .has("custom_number_property", "1")
                .hasNext()
        );

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

        assertTrue(g.traversal().V()
                .has(GremlinExecutionGraph.EVENT_ID, receive.getId())
                .has("custom_string_property", "value")
                .has("custom_number_property", "1")
                .hasNext()
        );

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

        assertTrue(g.traversal().V()
                .has(GremlinExecutionGraph.EVENT_ID, send.getId())
                .has("custom_string_property", "value")
                .has("custom_number_property", "1")
                .hasNext()
        );

        Log log = new Log();
        log.setId("snd");
        log.setUserTime(1);
        log.setKernelTime(3);
        log.setPid(9);
        log.setTid(10);
        log.setComm("comm");
        log.setHost("host2");
        log.setExtraData(extraData);
        graph.addEvent(log);

        assertTrue(g.traversal().V()
                .has(GremlinExecutionGraph.EVENT_ID, log.getId())
                .has("custom_string_property", "value")
                .has("custom_number_property", "1")
                .hasNext()
        );
    }

    @Test
    void addLogEventBetweenTwoExistingEvents() {
        JsonObject extraData = new JsonObject();
        extraData.add("message", new JsonPrimitive("creating new thread"));

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
        create2.setKernelTime(3);
        create2.setPid(9);
        create2.setTid(9);
        create2.setComm("comm");
        create2.setHost("host");
        create2.setChildPid(12);
        graph.addEvent(create2);

        Log log = new Log();
        log.setId("log");
        log.setUserTime(1);
        log.setKernelTime(2);
        log.setPid(9);
        log.setTid(9);
        log.setComm("comm");
        log.setHost("host");
        log.setExtraData(extraData);
        graph.addEvent(log);

        Graph g = graph.getGraph();

        assertTrue(g.traversal().V().has(GremlinExecutionGraph.EVENT_ID, create1.getId()).out(
                GremlinExecutionGraph.HAPPENS_BEFORE_LABEL).has(GremlinExecutionGraph.EVENT_ID,
                log.getId()).out(
                GremlinExecutionGraph.HAPPENS_BEFORE_LABEL).has(GremlinExecutionGraph.EVENT_ID,
                create2.getId()).hasNext());
    }

    @Test
    void latestTimestampsOfCompletedEventsAreUpdatedOnSend() {
        Graph g = graph.getGraph();

        Vertex rcv1 = g.addVertex(T.label, EventType.RCV.toString(), GremlinExecutionGraph.EVENT_ID, "rcv-1", "threadId",
                "9/9@host1", "size", 1,
                "kernelTime", 1, "socketId", "socket-id", "socketFamily", 10);
        Vertex rcv2 = g.addVertex(T.label, EventType.RCV.toString(), GremlinExecutionGraph.EVENT_ID, "rcv-2", "threadId",
                "9/9@host1", "size", 5,
                "kernelTime", 2, "socketId", "socket-id", "socketFamily", 10);
        Vertex rcv3 = g.addVertex(T.label, EventType.RCV.toString(), GremlinExecutionGraph.EVENT_ID, "rcv-3", "threadId",
                "9/9@host1", "size", 5,
                "kernelTime", 3, "socketId", "socket-id", "socketFamily", 10);
        Vertex rcv4 = g.addVertex(T.label, EventType.RCV.toString(), GremlinExecutionGraph.EVENT_ID, "rcv-4", "threadId",
                "9/9@host1", "size", 5,
                "kernelTime", 4, "socketId", "socket-id", "socketFamily", 10);
        Vertex rcv5 = g.addVertex(T.label, EventType.RCV.toString(), GremlinExecutionGraph.EVENT_ID, "rcv-5", "threadId",
                "9/9@host1", "size", 5,
                "kernelTime", 5, "socketId", "socket-id", "socketFamily", 10);
        Vertex snd1 = g.addVertex(T.label, EventType.SND.toString(), GremlinExecutionGraph.EVENT_ID, "snd-1", "threadId",
                "9/10@host2", "size", 1,
                "kernelTime", 1, "socketId", "socket-id", "socketFamily", 10);
        Vertex snd2 = g.addVertex(T.label, EventType.SND.toString(), GremlinExecutionGraph.EVENT_ID, "snd-2", "threadId",
                "9/10@host2", "size", 1,
                "kernelTime", 2, "socketId", "socket-id", "socketFamily", 10);

        rcv1.addEdge(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL, rcv2);
        rcv2.addEdge(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL, rcv3);
        rcv3.addEdge(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL, rcv4);
        rcv4.addEdge(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL, rcv5);
        snd1.addEdge(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL, rcv1, "size", 1);
        snd2.addEdge(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL, rcv2, "size", 1);

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
        sendEvent.setSocketFrom("0.0.0.0");
        sendEvent.setSourcePort(10);
        sendEvent.setSocketTo("0.0.0.0");
        sendEvent.setDestinationPort(10);
        sendEvent.setSize(12);
        graph.addEvent(sendEvent);

        assertEquals(sendEvent.getKernelTime(), graph.getLastCompleteSndKernelTime(sendEvent.getHost(), sendEvent.getSocketId()));
        assertEquals(((Integer) rcv4.property("kernelTime").value()).longValue(), graph.getLastCompleteRcvKernelTime("host2", rcv4.property("socketId").value().toString()));
    }

    @Test
    void latestTimestampsOfCompletedEventsAreUpdatedOnReceive() {
        Graph g = graph.getGraph();

        Vertex snd1 = g.addVertex(T.label, EventType.SND.toString(), GremlinExecutionGraph.EVENT_ID, "snd-1", "threadId",
                "9/9@host1", "size", 1,
                "kernelTime", 1, "socketId", "socket-id", "socketFamily", 10);
        Vertex snd2 = g.addVertex(T.label, EventType.SND.toString(), GremlinExecutionGraph.EVENT_ID, "snd-2", "threadId",
                "9/9@host1", "size", 5,
                "kernelTime", 2, "socketId", "socket-id", "socketFamily", 10);
        Vertex snd3 = g.addVertex(T.label, EventType.SND.toString(), GremlinExecutionGraph.EVENT_ID, "snd-3", "threadId",
                "9/9@host1", "size", 5,
                "kernelTime", 3, "socketId", "socket-id", "socketFamily", 10);
        Vertex snd4 = g.addVertex(T.label, EventType.SND.toString(), GremlinExecutionGraph.EVENT_ID, "snd-4", "threadId",
                "9/9@host1", "size", 5,
                "kernelTime", 4, "socketId", "socket-id", "socketFamily", 10);
        Vertex snd5 = g.addVertex(T.label, EventType.SND.toString(), GremlinExecutionGraph.EVENT_ID, "snd-5", "threadId",
                "9/9@host1", "size", 5,
                "kernelTime", 5, "socketId", "socket-id", "socketFamily", 10);
        Vertex rcv1 = g.addVertex(T.label, EventType.RCV.toString(), GremlinExecutionGraph.EVENT_ID, "rcv-1", "threadId",
                "9/10@host2", "size", 1,
                "kernelTime", 1, "socketId", "socket-id", "socketFamily", 10);
        Vertex rcv2 = g.addVertex(T.label, EventType.RCV.toString(), GremlinExecutionGraph.EVENT_ID, "rcv-2", "threadId",
                "9/10@host2", "size", 1,
                "kernelTime", 2, "socketId", "socket-id", "socketFamily", 10);

        snd1.addEdge(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL, snd2);
        snd2.addEdge(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL, snd3);
        snd3.addEdge(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL, snd4);
        snd4.addEdge(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL, snd5);
        snd1.addEdge(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL, rcv1, "size", 1);
        snd2.addEdge(GremlinExecutionGraph.HAPPENS_BEFORE_LABEL, rcv2, "size", 1);

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
        receiveEvent.setSocketFrom("0.0.0.0");
        receiveEvent.setSourcePort(10);
        receiveEvent.setSocketTo("0.0.0.0");
        receiveEvent.setDestinationPort(10);
        receiveEvent.setSize(12);
        graph.addEvent(receiveEvent);

        assertEquals(receiveEvent.getKernelTime(), graph.getLastCompleteRcvKernelTime(receiveEvent.getHost(), receiveEvent.getSocketId()));
        assertEquals(((Integer) snd4.property("kernelTime").value()).longValue(), graph.getLastCompleteSndKernelTime("host2", snd4.property("socketId").value().toString()));
    }

}
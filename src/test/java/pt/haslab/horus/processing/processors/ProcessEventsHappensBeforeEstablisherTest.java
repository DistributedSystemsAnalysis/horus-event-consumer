package pt.haslab.horus.processing.processors;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pt.haslab.horus.events.*;

import java.util.ArrayList;
import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ProcessEventsHappensBeforeEstablisherTest {

    private ProcessEventsHappensBeforeEstablisher establisher;

    @BeforeEach
    void setUp() {
        this.establisher = new ProcessEventsHappensBeforeEstablisher();
    }

    /**
     * This test ensures that the generic method establish() works as expected.
     * It was raising exceptions due to null returns.
     */
    @Test
    void establishCreateAndStartRelationshipUsingGenericMethod() {
        ProcessCreate createEvent = new ProcessCreate();
        createEvent.setId("create");
        createEvent.setUserTime(1);
        createEvent.setKernelTime(1);
        createEvent.setPid(9);
        createEvent.setTid(9);
        createEvent.setComm("comm");
        createEvent.setHost("host");
        createEvent.setChildPid(10);

        ProcessStart startEvent = new ProcessStart();
        startEvent.setId("start");
        startEvent.setUserTime(1);
        startEvent.setKernelTime(2);
        startEvent.setPid(9);
        startEvent.setTid(10);
        startEvent.setComm("comm");
        startEvent.setHost("host");

        assertEquals(0, this.establisher.establish((Event) createEvent).size());

        Collection<Pair<Event, Event>> foundPairs = this.establisher.establish((Event) startEvent);
        assertEquals(1, foundPairs.size());

        ArrayList<Pair<Event, Event>> foundPairsList = new ArrayList<Pair<Event, Event>>(foundPairs);
        Pair<Event, Event> foundPair = foundPairsList.get(0);
        assertEquals(foundPair.getLeft().getId(), createEvent.getId());
        assertEquals(foundPair.getRight().getId(), startEvent.getId());
    }

    @Test
    void establishCreateAndStartRelationship() {
        ProcessCreate createEvent = new ProcessCreate();
        createEvent.setId("create");
        createEvent.setUserTime(1);
        createEvent.setKernelTime(1);
        createEvent.setPid(9);
        createEvent.setTid(9);
        createEvent.setComm("comm");
        createEvent.setHost("host");
        createEvent.setChildPid(10);

        ProcessStart startEvent = new ProcessStart();
        startEvent.setId("start");
        startEvent.setUserTime(1);
        startEvent.setKernelTime(2);
        startEvent.setPid(9);
        startEvent.setTid(10);
        startEvent.setComm("comm");
        startEvent.setHost("host");

        assertNull(this.establisher.establish(createEvent));

        Pair<ProcessCreate, ProcessStart> foundPair = this.establisher.establish(startEvent);
        assertEquals(foundPair.getLeft().getId(), createEvent.getId());
        assertEquals(foundPair.getRight().getId(), startEvent.getId());
    }

    @Test
    void establishCreateAndStartRelationshipOnInverseOrder() {
        ProcessCreate createEvent = new ProcessCreate();
        createEvent.setId("create");
        createEvent.setUserTime(1);
        createEvent.setKernelTime(1);
        createEvent.setPid(9);
        createEvent.setTid(9);
        createEvent.setComm("comm");
        createEvent.setHost("host");
        createEvent.setChildPid(10);

        ProcessStart startEvent = new ProcessStart();
        startEvent.setId("start");
        startEvent.setUserTime(1);
        startEvent.setKernelTime(2);
        startEvent.setPid(9);
        startEvent.setTid(10);
        startEvent.setComm("comm");
        startEvent.setHost("host");

        assertNull(this.establisher.establish(startEvent));

        Pair<ProcessCreate, ProcessStart> foundPair = this.establisher.establish(createEvent);
        assertEquals(foundPair.getLeft().getId(), createEvent.getId());
        assertEquals(foundPair.getRight().getId(), startEvent.getId());
    }

    @Test
    void establishEndAndJoinRelationship() {
        ProcessJoin joinEvent = new ProcessJoin();
        joinEvent.setId("join");
        joinEvent.setUserTime(1);
        joinEvent.setKernelTime(1);
        joinEvent.setPid(9);
        joinEvent.setTid(9);
        joinEvent.setComm("comm");
        joinEvent.setHost("host");
        joinEvent.setChildPid(10);

        ProcessEnd endEvent = new ProcessEnd();
        endEvent.setId("end");
        endEvent.setUserTime(1);
        endEvent.setKernelTime(2);
        endEvent.setPid(9);
        endEvent.setTid(10);
        endEvent.setComm("comm");
        endEvent.setHost("host");

        assertNull(this.establisher.establish(endEvent));

        Pair<ProcessEnd, ProcessJoin> foundPair = this.establisher.establish(joinEvent);
        assertEquals(foundPair.getLeft().getId(), endEvent.getId());
        assertEquals(foundPair.getRight().getId(), joinEvent.getId());
    }

    @Test
    void establishEndAndJoinRelationshipOnInverseOrder() {
        ProcessJoin joinEvent = new ProcessJoin();
        joinEvent.setId("join");
        joinEvent.setUserTime(1);
        joinEvent.setKernelTime(1);
        joinEvent.setPid(9);
        joinEvent.setTid(9);
        joinEvent.setComm("comm");
        joinEvent.setHost("host");
        joinEvent.setChildPid(10);

        ProcessEnd endEvent = new ProcessEnd();
        endEvent.setId("end");
        endEvent.setUserTime(1);
        endEvent.setKernelTime(2);
        endEvent.setPid(9);
        endEvent.setTid(10);
        endEvent.setComm("comm");
        endEvent.setHost("host");

        assertNull(this.establisher.establish(endEvent));

        Pair<ProcessEnd, ProcessJoin> foundPair = this.establisher.establish(joinEvent);
        assertEquals(foundPair.getLeft().getId(), endEvent.getId());
        assertEquals(foundPair.getRight().getId(), joinEvent.getId());
    }
}
package pt.haslab.horus.processing.processors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pt.haslab.horus.events.*;

import java.util.ArrayList;
import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class SocketEventsHappensBeforeEstablisherTest {

    private SocketEventsHappensBeforeEstablisher establisher;

    @BeforeEach
    void setUp() {
        this.establisher = new SocketEventsHappensBeforeEstablisher();
    }

    @Test
    void establishConnectAndAcceptRelationships() {
        SocketConnect connectEvent = new SocketConnect();
        connectEvent.setId("connect");
        connectEvent.setUserTime(1);
        connectEvent.setKernelTime(3);
        connectEvent.setPid(9);
        connectEvent.setTid(10);
        connectEvent.setComm("comm");
        connectEvent.setHost("host");
        connectEvent.setSocketId("socket-id");
        connectEvent.setSocketFamily(10);
        connectEvent.setSocketFrom("0.0.0.0");
        connectEvent.setSourcePort(10);
        connectEvent.setSocketTo("0.0.0.0");
        connectEvent.setDestinationPort(10);

        SocketAccept acceptEvent = new SocketAccept();
        acceptEvent.setId("accept");
        acceptEvent.setUserTime(1);
        acceptEvent.setKernelTime(3);
        acceptEvent.setPid(9);
        acceptEvent.setTid(10);
        acceptEvent.setComm("comm");
        acceptEvent.setHost("host2");
        acceptEvent.setSocketId("socket-id");
        acceptEvent.setSocketFamily(10);
        acceptEvent.setSocketFrom("0.0.0.0");
        acceptEvent.setSourcePort(10);
        acceptEvent.setSocketTo("0.0.0.0");
        acceptEvent.setDestinationPort(10);

        assertNull(this.establisher.establish(connectEvent));

        Pair<SocketConnect, SocketAccept> foundPair = this.establisher.establish(acceptEvent);
        assertEquals(foundPair.getRight().getId(), acceptEvent.getId());
    }

    @Test
    void establishConnectAndAcceptRelationshipsOnInverseOrder() {
        SocketConnect connectEvent = new SocketConnect();
        connectEvent.setId("connect");
        connectEvent.setUserTime(1);
        connectEvent.setKernelTime(3);
        connectEvent.setPid(9);
        connectEvent.setTid(10);
        connectEvent.setComm("comm");
        connectEvent.setHost("host");
        connectEvent.setSocketId("socket-id");
        connectEvent.setSocketFamily(10);
        connectEvent.setSocketFrom("0.0.0.0");
        connectEvent.setSourcePort(10);
        connectEvent.setSocketTo("0.0.0.0");
        connectEvent.setDestinationPort(10);

        SocketAccept acceptEvent = new SocketAccept();
        acceptEvent.setId("accept");
        acceptEvent.setUserTime(1);
        acceptEvent.setKernelTime(3);
        acceptEvent.setPid(9);
        acceptEvent.setTid(10);
        acceptEvent.setComm("comm");
        acceptEvent.setHost("host2");
        acceptEvent.setSocketId("socket-id");
        acceptEvent.setSocketFamily(10);
        acceptEvent.setSocketFrom("0.0.0.0");
        acceptEvent.setSourcePort(10);
        acceptEvent.setSocketTo("0.0.0.0");
        acceptEvent.setDestinationPort(10);

        assertNull(this.establisher.establish(acceptEvent));

        Pair<SocketConnect, SocketAccept> foundPair = this.establisher.establish(connectEvent);
        assertEquals(foundPair.getRight().getId(), acceptEvent.getId());
    }

    @Test
    void establishSendAndReceiveRelationships() {
        ArrayList<SocketSend> sendEvents = EventUtil.getSendEvents(10, 10);
        ArrayList<SocketReceive> receiveEvents = EventUtil.getReceiveEvents(5, 10, 5);
        Pair<SocketSend, Collection<SocketReceive>> establishedSendRelationships = null;
        Pair<Collection<SocketSend>, SocketReceive> establishedReceiveRelationships = null;
        ArrayList<SocketSend> foundSendMatches = null;

        establishedSendRelationships = this.establisher.establish(sendEvents.get(0));
        assertEquals(0, establishedSendRelationships.getRight().size());

        establishedSendRelationships = this.establisher.establish(sendEvents.get(1));
        assertEquals(0, establishedSendRelationships.getRight().size());

        establishedReceiveRelationships = this.establisher.establish(receiveEvents.get(0));
        assertEquals(1, establishedReceiveRelationships.getLeft().size());

        foundSendMatches = new ArrayList<>(establishedReceiveRelationships.getLeft());
        assertEquals(sendEvents.get(0).getId(), foundSendMatches.get(0).getId());

        establishedReceiveRelationships = this.establisher.establish(receiveEvents.get(1));
        assertEquals(2, establishedReceiveRelationships.getLeft().size());

        foundSendMatches = new ArrayList<>(establishedReceiveRelationships.getLeft());
        assertEquals(sendEvents.get(0).getId(), foundSendMatches.get(0).getId());
        assertEquals(sendEvents.get(1).getId(), foundSendMatches.get(1).getId());

        establishedReceiveRelationships = this.establisher.establish(receiveEvents.get(2));
        assertEquals(1, establishedReceiveRelationships.getLeft().size());

        foundSendMatches = new ArrayList<>(establishedReceiveRelationships.getLeft());
        assertEquals(sendEvents.get(1).getId(), foundSendMatches.get(0).getId());
    }

    @Test
    void establishAndUnrollSendAndReceiveRelationships() {
        ArrayList<SocketSend> sendEvents = EventUtil.getSendEvents(10, 10);
        ArrayList<SocketReceive> receiveEvents = EventUtil.getReceiveEvents(5, 10, 5);
        ArrayList<Pair<Event, Event>> expectedRelationships = new ArrayList<Pair<Event, Event>>() {
            {
                add(new ImmutablePair<>(sendEvents.get(0), receiveEvents.get(0)));
                add(new ImmutablePair<>(sendEvents.get(0), receiveEvents.get(1)));
                add(new ImmutablePair<>(sendEvents.get(1), receiveEvents.get(1)));
                add(new ImmutablePair<>(sendEvents.get(1), receiveEvents.get(2)));
            }
        };

        ArrayList<Pair<Event, Event>> actualRelationships = new ArrayList<>();

        actualRelationships.addAll(this.establisher.establish((Event) sendEvents.get(0)));
        actualRelationships.addAll(this.establisher.establish((Event) sendEvents.get(1)));
        actualRelationships.addAll(this.establisher.establish((Event) receiveEvents.get(0)));
        actualRelationships.addAll(this.establisher.establish((Event) receiveEvents.get(1)));
        actualRelationships.addAll(this.establisher.establish((Event) receiveEvents.get(2)));

        assertEquals(expectedRelationships.size(), actualRelationships.size());

        for (int i = 0; i < expectedRelationships.size(); i++) {
            Pair<Event, Event> expectedRelationship = expectedRelationships.get(i);
            Pair<Event, Event> actualRelationship = actualRelationships.get(i);

            assertEquals(expectedRelationship, actualRelationship);
        }
    }
}
package pt.haslab.horus.processing.processors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import pt.haslab.horus.events.*;

import java.util.*;

public class SocketEventsHappensBeforeEstablisher implements EventRelationshipEstablisher {
    private HashMap<String, SocketConnect> pendingConnectEvents;
    private HashMap<String, SocketAccept> pendingAcceptEvents;
    private HashMap<String, Deque<PendingSocketSend>> pendingSocketSendEvents;
    private HashMap<String, Deque<PendingSocketReceive>> pendingSocketReceiveEvents;

    public SocketEventsHappensBeforeEstablisher() {
        this.pendingConnectEvents = new HashMap<>();
        this.pendingAcceptEvents = new HashMap<>();
        this.pendingSocketSendEvents = new HashMap<>();
        this.pendingSocketReceiveEvents = new HashMap<>();
    }

    public Pair<SocketConnect, SocketAccept> establish(SocketConnect connectEvent) {
        SocketAccept match = this.pendingAcceptEvents.remove(connectEvent.getSocketId());

        if (match != null) {
            return new ImmutablePair<>(connectEvent, match);
        }

        this.pendingConnectEvents.put(connectEvent.getSocketId(), connectEvent);

        return null;
    }

    public Pair<SocketConnect, SocketAccept> establish(SocketAccept acceptEvent) {
        SocketConnect match = this.pendingConnectEvents.remove(acceptEvent.getSocketId());

        if (match != null) {
            return new ImmutablePair<>(match, acceptEvent);
        }

        this.pendingAcceptEvents.put(acceptEvent.getSocketId(), acceptEvent);

        return null;
    }

    public Pair<SocketSend, Collection<SocketReceive>> establish(SocketSend sendEvent) {
        Collection<SocketReceive> matches = this.findReceiveMatches(sendEvent);

        return new ImmutablePair<>(sendEvent, matches);
    }

    public Pair<Collection<SocketSend>, SocketReceive> establish(SocketReceive receiveEvent) {
        Collection<SocketSend> matches = this.findSendMatches(receiveEvent);

        return new ImmutablePair<>(matches, receiveEvent);
    }

    private Collection<SocketReceive> findReceiveMatches(SocketSend sendEvent) {
        List<SocketReceive> matches = new ArrayList<>();
        PendingSocketSend pendingSend = new PendingSocketSend(sendEvent);
        String channelStreamId = getChannelStreamId(sendEvent);

        Deque<PendingSocketReceive> matchCandidates = this.getPendingReceivesForStream(channelStreamId);
        while (!pendingSend.isComplete() && matchCandidates.size() > 0) {
            PendingSocketReceive currentPendingReceive = matchCandidates.peek();

            this.updateRemainingSizes(pendingSend, currentPendingReceive);

            if (currentPendingReceive.isComplete())
                matchCandidates.remove();

            matches.add(currentPendingReceive.getOriginalEvent());
        }

        if (!pendingSend.isComplete()) {
            this.enqueueSendEvent(pendingSend);
        }

        return matches;
    }

    private Collection<SocketSend> findSendMatches(SocketReceive receiveEvent) {
        List<SocketSend> matches = new ArrayList<>();
        PendingSocketReceive pendingReceive = new PendingSocketReceive(receiveEvent);
        String channelStreamId = getChannelStreamId(receiveEvent);

        Deque<PendingSocketSend> matchCandidates = this.getPendingSendsForStream(channelStreamId);
        while (!pendingReceive.isComplete() && matchCandidates.size() > 0) {
            PendingSocketSend currentPendingSend = matchCandidates.peek();

            this.updateRemainingSizes(currentPendingSend, pendingReceive);

            if (currentPendingSend.isComplete())
                matchCandidates.remove();

            matches.add(currentPendingSend.getOriginalEvent());
        }

        if (!pendingReceive.isComplete()) {
            this.enqueueReceiveEvent(pendingReceive);
        }

        return matches;
    }

    private Deque<PendingSocketSend> getPendingSendsForStream(String channelStreamId) {
        Deque<PendingSocketSend> pendingSends = null;

        if ((pendingSends = this.pendingSocketSendEvents.get(channelStreamId)) != null) {
            return pendingSends;
        }

        this.pendingSocketSendEvents.put(channelStreamId, pendingSends = new ArrayDeque<>());

        return pendingSends;
    }

    private Deque<PendingSocketReceive> getPendingReceivesForStream(String channelStreamId) {
        Deque<PendingSocketReceive> pendingReceives = null;

        if ((pendingReceives = this.pendingSocketReceiveEvents.get(channelStreamId)) != null) {
            return pendingReceives;
        }

        this.pendingSocketReceiveEvents.put(channelStreamId, pendingReceives = new ArrayDeque<>());

        return pendingReceives;
    }

    private void enqueueSendEvent(PendingSocketSend pendingSend) {
        String channelStreamId = getChannelStreamId(pendingSend.getOriginalEvent());
        Deque<PendingSocketSend> pendingSendEvents = this.getPendingSendsForStream(channelStreamId);
        pendingSendEvents.add(pendingSend);

        this.pendingSocketSendEvents.putIfAbsent(channelStreamId, pendingSendEvents);
    }

    private void enqueueReceiveEvent(PendingSocketReceive pendingReceive) {
        String channelStreamId = getChannelStreamId(pendingReceive.getOriginalEvent());
        Deque<PendingSocketReceive> pendingReceiveEvents = this.getPendingReceivesForStream(channelStreamId);
        pendingReceiveEvents.add(pendingReceive);

        this.pendingSocketReceiveEvents.putIfAbsent(channelStreamId, pendingReceiveEvents);
    }

    private void updateRemainingSizes(PendingSocketSend pendingSend, PendingSocketReceive pendingReceive) {
        int matchedBytes = pendingSend.decrementSize(pendingReceive.getRemainingSize());

        pendingReceive.decrementSize(matchedBytes);
    }

    @Override
    public Collection<Pair<Event, Event>> establish(Event event) {
        List<Pair<Event, Event>> relationships = new ArrayList<>();

        if (event instanceof SocketConnect) {
            Pair<SocketConnect, SocketAccept> foundRelationship = this.establish((SocketConnect) event);
            if (foundRelationship != null) {
                relationships.add(new ImmutablePair<>(foundRelationship.getLeft(), foundRelationship.getRight()));
            }
        } else if (event instanceof SocketAccept) {
            Pair<SocketConnect, SocketAccept> foundRelationship = this.establish((SocketAccept) event);
            if (foundRelationship != null) {
                relationships.add(new ImmutablePair<>(foundRelationship.getLeft(), foundRelationship.getRight()));
            }
        } else if (event instanceof SocketSend) {
            Pair<SocketSend, Collection<SocketReceive>> foundRelationships = this.establish((SocketSend) event);

            foundRelationships.getRight().forEach((receive) -> {
                relationships.add(new ImmutablePair<>(foundRelationships.getLeft(), receive));
            });
        } else if (event instanceof SocketReceive) {
            Pair<Collection<SocketSend>, SocketReceive> foundRelationships = this.establish((SocketReceive) event);

            foundRelationships.getLeft().forEach((send) -> {
                relationships.add(new ImmutablePair<>(send, foundRelationships.getRight()));
            });
        } else {
            throw new IllegalArgumentException("Unexpected event type.");
        }

        return relationships;
    }

    private static String getChannelStreamId(SocketEvent event) {
        return String.format("%s:%s-%s:%s", event.getSocketFrom(), event.getSourcePort(), event.getSocketTo(), event.getDestinationPort());
    }
}

abstract class PendingSocketEvent {
    private int remainingSize;

    public PendingSocketEvent(int remainingSize) {
        this.remainingSize = remainingSize;
    }

    public int getRemainingSize() {
        return this.remainingSize;
    }

    public boolean isComplete() {
        return this.remainingSize == 0;
    }

    public int decrementSize(int amount) {
        int decrementValue = Math.min(this.remainingSize, amount);

        this.remainingSize -= decrementValue;

        return decrementValue;
    }
}

class PendingSocketSend extends PendingSocketEvent {
    private final SocketSend send;

    public PendingSocketSend(SocketSend send) {
        super(send.getSize());
        this.send = send;
    }

    public SocketSend getOriginalEvent() {
        return this.send;
    }
}

class PendingSocketReceive extends PendingSocketEvent {
    private final SocketReceive receive;

    public PendingSocketReceive(SocketReceive receive) {
        super(receive.getSize());
        this.receive = receive;
    }

    public SocketReceive getOriginalEvent() {
        return this.receive;
    }
}

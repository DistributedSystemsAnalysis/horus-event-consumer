package pt.haslab.horus.processing.processors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import pt.haslab.horus.events.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

public class ProcessEventsHappensBeforeEstablisher implements EventRelationshipEstablisher {
    private HashMap<String, ProcessCreate> pendingCreateEvents;
    private HashMap<String, ProcessStart> pendingStartEvents;
    private HashMap<String, ProcessEnd> pendingEndEvents;
    private HashMap<String, ProcessJoin> pendingJoinEvents;

    public ProcessEventsHappensBeforeEstablisher() {
        this.pendingCreateEvents = new HashMap<>();
        this.pendingStartEvents = new HashMap<>();
        this.pendingEndEvents = new HashMap<>();
        this.pendingJoinEvents = new HashMap<>();
    }

    public Pair<ProcessCreate, ProcessStart> establish(ProcessStart startEvent) {
        String commonEventId = generateProcessEventId(startEvent);

        ProcessCreate match = this.pendingCreateEvents.remove(commonEventId);

        if (match != null) {
            return new ImmutablePair<>(match, startEvent);
        }

        this.pendingStartEvents.put(commonEventId, startEvent);

        return null;
    }

    public Pair<ProcessCreate, ProcessStart> establish(ProcessCreate createEvent) {
        String commonEventId = generateProcessEventId(createEvent);

        ProcessStart match = this.pendingStartEvents.remove(commonEventId);

        if (match != null) {
            return new ImmutablePair<>(createEvent, match);
        }

        this.pendingCreateEvents.put(commonEventId, createEvent);

        return null;
    }

    public Pair<ProcessEnd, ProcessJoin> establish(ProcessJoin joinEvent) {
        String commonEventId = generateProcessEventId(joinEvent);

        ProcessEnd match = this.pendingEndEvents.remove(commonEventId);

        if (match != null) {
            return new ImmutablePair<>(match, joinEvent);
        }

        this.pendingJoinEvents.put(commonEventId, joinEvent);

        return null;
    }

    public Pair<ProcessEnd, ProcessJoin> establish(ProcessEnd endEvent) {
        String commonEventId = generateProcessEventId(endEvent);

        ProcessJoin match = this.pendingJoinEvents.remove(commonEventId);

        if (match != null) {
            return new ImmutablePair<>(endEvent, match);
        }

        this.pendingEndEvents.put(commonEventId, endEvent);

        return null;
    }

    private static String generateProcessEventId(Event event) {
        if (event instanceof ProcessCreate) {
            return event.getHost() + "-" + ((ProcessCreate) event).getChildPid();
        }

        if (event instanceof ProcessStart) {
            return event.getHost() + "-" + event.getTid();
        }

        if (event instanceof ProcessJoin) {
            return event.getHost() + "-" + ((ProcessJoin) event).getChildPid();
        }

        if (event instanceof ProcessEnd) {
            return event.getHost() + "-" + event.getTid();
        }

        throw new IllegalArgumentException("Unrecognized event type.");
    }

    @Override
    public Collection<Pair<Event, Event>> establish(Event event) {
        List<Pair<Event, Event>> relationships = new ArrayList<>();

        if (event instanceof ProcessCreate) {
            Pair<ProcessCreate, ProcessStart> foundRelationship = this.establish((ProcessCreate) event);

            if (foundRelationship != null) {
                relationships.add(new ImmutablePair<>(foundRelationship.getLeft(), foundRelationship.getRight()));
            }
        }

        if (event instanceof ProcessStart) {
            Pair<ProcessCreate, ProcessStart> foundRelationship = this.establish((ProcessStart) event);

            if (foundRelationship != null) {
                relationships.add(new ImmutablePair<>(foundRelationship.getLeft(), foundRelationship.getRight()));
            }
        }

        if (event instanceof ProcessJoin) {
            Pair<ProcessEnd, ProcessJoin> foundRelationship = this.establish((ProcessJoin) event);

            if (foundRelationship != null) {
                relationships.add(new ImmutablePair<>(foundRelationship.getLeft(), foundRelationship.getRight()));
            }
        }

        if (event instanceof ProcessEnd) {
            Pair<ProcessEnd, ProcessJoin> foundRelationship = this.establish((ProcessEnd) event);

            if (foundRelationship != null) {
                relationships.add(new ImmutablePair<>(foundRelationship.getLeft(), foundRelationship.getRight()));
            }
        }

        return relationships;
    }
}

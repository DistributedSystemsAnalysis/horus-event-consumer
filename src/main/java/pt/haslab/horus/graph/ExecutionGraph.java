package pt.haslab.horus.graph;

import org.apache.commons.lang3.tuple.Pair;
import pt.haslab.horus.events.*;
import pt.haslab.horus.graph.timeline.TimelineIdGenerator;

import java.util.Collection;
import java.util.List;

public interface ExecutionGraph {
    void addEvent(ProcessCreate event);

    void addEvent(ProcessStart event);

    void addEvent(ProcessEnd event);

    void addEvent(ProcessJoin event);

    void addEvent(SocketAccept event);

    void addEvent(SocketConnect event);

    void addEvent(SocketSend event);

    void addEvent(SocketReceive event);

    void addEvent(Log event);

    void addEvent(FSync event);

    void addEvent(Event event);

    TimelineIdGenerator getTimelineIdGenerator();

    void storeEvents(List<Event> events);

    void storeRelationships(Collection<Pair<Event, Event>> relationships);
}

package pt.haslab.horus.graph.datastores;

import org.apache.commons.lang3.tuple.Pair;
import pt.haslab.horus.events.*;
import pt.haslab.horus.graph.ExecutionGraph;
import pt.haslab.horus.graph.timeline.ProcessTimelineIdGenerator;
import pt.haslab.horus.graph.timeline.TimelineIdGenerator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class MemoryExecutionGraph implements ExecutionGraph {
    private List<Event> storedEvents;
    private List<Pair<Event, Event>> storedRelationships;

    public MemoryExecutionGraph() {
        this.storedEvents = new ArrayList<>();
        this.storedRelationships = new ArrayList<>();
    }

    @Override
    public void addEvent(ProcessCreate event) {
        this.storedEvents.add(event);
    }

    @Override
    public void addEvent(ProcessStart event) {
        this.storedEvents.add(event);
    }

    @Override
    public void addEvent(ProcessEnd event) {
        this.storedEvents.add(event);
    }

    @Override
    public void addEvent(ProcessJoin event) {
        this.storedEvents.add(event);
    }

    @Override
    public void addEvent(SocketAccept event) {
        this.storedEvents.add(event);
    }

    @Override
    public void addEvent(SocketConnect event) {
        this.storedEvents.add(event);
    }

    @Override
    public void addEvent(SocketSend event) {
        this.storedEvents.add(event);
    }

    @Override
    public void addEvent(SocketReceive event) {
        this.storedEvents.add(event);
    }

    @Override
    public void addEvent(Log event) {
        this.storedEvents.add(event);
    }

    @Override
    public void addEvent(FSync event) {
        this.storedEvents.add(event);
    }

    @Override
    public void addEvent(Event event) {
        this.storedEvents.add(event);
    }

    @Override
    public TimelineIdGenerator getTimelineIdGenerator() {
        return new ProcessTimelineIdGenerator();
    }

    @Override
    public synchronized void storeEvents(List<Event> events) {
        this.storedEvents.addAll(events);
    }

    public Collection<Event> getEvents() {
        return this.storedEvents;
    }

    @Override
    public synchronized void storeRelationships(Collection<Pair<Event, Event>> relationships) {
        this.storedRelationships.addAll(relationships);
    }

    public Collection<Pair<Event, Event>> getRelationships() {
        return this.storedRelationships;
    }
}

package pt.haslab.horus.graph.datastores;

import org.apache.commons.lang3.tuple.Pair;
import pt.haslab.horus.events.*;
import pt.haslab.horus.graph.AbstractExecutionGraph;
import pt.haslab.horus.graph.ExecutionGraph;
import pt.haslab.horus.graph.timeline.ProcessTimelineIdGenerator;
import pt.haslab.horus.graph.timeline.TimelineIdGenerator;
import pt.haslab.horus.graph.timeline.TimelineIdGeneratorFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class NoopExecutionGraph extends AbstractExecutionGraph {

    public NoopExecutionGraph() {
        this.timelineIdGenerator = TimelineIdGeneratorFactory.get();
    }

    @Override
    public void addEvent(ProcessCreate event) {
        //
    }

    @Override
    public void addEvent(ProcessStart event) {
        //
    }

    @Override
    public void addEvent(ProcessEnd event) {
        //
    }

    @Override
    public void addEvent(ProcessJoin event) {
        //
    }

    @Override
    public void addEvent(SocketAccept event) {
        //
    }

    @Override
    public void addEvent(SocketConnect event) {
        //
    }

    @Override
    public void addEvent(SocketSend event) {
        //
    }

    @Override
    public void addEvent(SocketReceive event) {
        //
    }

    @Override
    public void addEvent(Log event) {
        //
    }

    @Override
    public void addEvent(FSync event) {
        //
    }

    @Override
    public void addEvent(Event event) {
        //
    }

    @Override
    public void storeEvents(List<Event> events) {
        //
    }

    @Override
    public void storeRelationships(Collection<Pair<Event, Event>> relationships) {
        //
    }

    @Override
    public void close() throws Exception {
        //
    }
}

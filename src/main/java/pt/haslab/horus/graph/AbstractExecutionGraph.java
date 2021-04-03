package pt.haslab.horus.graph;

import pt.haslab.horus.graph.timeline.TimelineIdGenerator;

public abstract class AbstractExecutionGraph implements ExecutionGraph, AutoCloseable {
    protected TimelineIdGenerator timelineIdGenerator;

    public TimelineIdGenerator getTimelineIdGenerator() {
        return this.timelineIdGenerator;
    }

    public void setTimelineIdGenerator(TimelineIdGenerator timelineIdGenerator) {
        this.timelineIdGenerator = timelineIdGenerator;
    }
}

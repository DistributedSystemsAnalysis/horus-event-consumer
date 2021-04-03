package pt.haslab.horus.graph.timeline;

import pt.haslab.horus.events.Event;

public class ProcessTimelineIdGenerator implements TimelineIdGenerator {
    @Override
    public String getTimelineId(Event event) {
        return String.format("%s@%s", event.getPid(), event.getHost());
    }
}

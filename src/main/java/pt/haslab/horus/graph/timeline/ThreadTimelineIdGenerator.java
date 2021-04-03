package pt.haslab.horus.graph.timeline;

import pt.haslab.horus.events.Event;

public class ThreadTimelineIdGenerator implements TimelineIdGenerator {
    @Override
    public String getTimelineId(Event event) {
        return String.format("%s@%s", event.getTid(), event.getHost());
    }
}

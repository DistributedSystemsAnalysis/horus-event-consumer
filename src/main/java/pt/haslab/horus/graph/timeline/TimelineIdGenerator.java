package pt.haslab.horus.graph.timeline;

import pt.haslab.horus.events.Event;

public interface TimelineIdGenerator {
    String getTimelineId(Event event);
}

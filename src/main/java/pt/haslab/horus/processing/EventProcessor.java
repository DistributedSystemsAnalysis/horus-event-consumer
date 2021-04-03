package pt.haslab.horus.processing;

import pt.haslab.horus.events.Event;

public interface EventProcessor {
    void process(Event event);
}

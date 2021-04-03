package pt.haslab.horus.processing.processors;

import pt.haslab.horus.events.Event;

public class OutOfOrderException extends Throwable {
    Event event;
    String timelineId;

    public OutOfOrderException(Event event, String timelineId) {
        super("Event ["+event.getType().getLabel()+":"+event.getId()+"] for timeline ["+timelineId+"] is out of order.");

        this.event = event;
        this.timelineId = timelineId;
    }

    public Event getEvent() {
        return this.event;
    }
}

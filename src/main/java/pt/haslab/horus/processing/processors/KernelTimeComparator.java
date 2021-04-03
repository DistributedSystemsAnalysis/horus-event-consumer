package pt.haslab.horus.processing.processors;

import pt.haslab.horus.events.Event;

import java.util.Comparator;

public class KernelTimeComparator implements Comparator<Event> {
    @Override
    public int compare(Event event1, Event event2) {
        return Long.compare(event1.getKernelTime(), event2.getKernelTime());
    }
}

package pt.haslab.horus.processing.processors;

import org.apache.commons.lang3.tuple.Pair;
import pt.haslab.horus.events.Event;

import java.util.Collection;

public interface EventRelationshipEstablisher {
    Collection<Pair<Event, Event>> establish(Event e);
}

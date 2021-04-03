package pt.haslab.horus.processing;

import com.conversantmedia.util.concurrent.PushPullBlockingQueue;
import pt.haslab.horus.events.Event;
import pt.haslab.horus.graph.ExecutionGraph;
import pt.haslab.horus.processing.processors.EventHappensBeforeRelationProcessor;
import pt.haslab.horus.processing.processors.TimelineEventAppenderProcessor;
import pt.haslab.horus.util.NamedThreadFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SyncEventProcessor implements EventProcessor {
    private ExecutionGraph graph;

    public SyncEventProcessor(ExecutionGraph graph) {
        this.graph = graph;
    }

    public void process(Event event) {
        this.graph.addEvent(event);
    }
}


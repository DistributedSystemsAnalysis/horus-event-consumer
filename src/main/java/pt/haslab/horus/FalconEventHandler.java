package pt.haslab.horus;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.haslab.horus.events.*;
import pt.haslab.horus.events.flatbuffers.generated.FalconEvent;
import pt.haslab.horus.graph.ExecutionGraph;
import pt.haslab.horus.processing.EventProcessor;

public class FalconEventHandler {
    final static Logger logger = LogManager.getLogger(FalconEventHandler.class);

    private final EventProcessor processor;
    private final MetricRegistry statsRegistry;

    public FalconEventHandler(EventProcessor processor) {
        this.processor = processor;
        this.statsRegistry = SharedMetricRegistries.getOrCreate("stats");
    }

    public void handle(FalconEvent falconEvent) {
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Received event with type [" + falconEvent.type() + "] with sub-type ["
                        + falconEvent.eventType() + "]");
            }

            switch (EventType.getEventType(falconEvent.type())) {
                case CREATE:
                    this.processor.process(new ProcessCreate(falconEvent));
                    break;
                case START:
                    this.processor.process(new ProcessStart(falconEvent));
                    break;
                case END:
                    this.processor.process(new ProcessEnd(falconEvent));
                    break;
                case JOIN:
                    this.processor.process(new ProcessJoin(falconEvent));
                    break;
                case CONNECT:
                    this.processor.process(new SocketConnect(falconEvent));
                    break;
                case ACCEPT:
                    this.processor.process(new SocketAccept(falconEvent));
                    break;
                case SND:
                    this.processor.process(new SocketSend(falconEvent));
                    break;
                case RCV:
                    this.processor.process(new SocketReceive(falconEvent));
                    break;
                case LOG:
                    this.processor.process(new Log(falconEvent));
                    break;
                case FSYNC:
                    this.processor.process(new FSync(falconEvent));
                    break;
                default:
                    logger.info("Unrecognized event type [" + falconEvent.type() + "]");
            }

            this.statsRegistry.counter(MetricRegistry.name(FalconEventHandler.class, "receivedEvents")).inc();

        } catch (IllegalArgumentException e) {
            logger.info("Message payload is invalid: " + e.getMessage());
        } catch (Exception e) {
            logger.error("Unexpected behavior.");
            e.printStackTrace();
        }
    }
}

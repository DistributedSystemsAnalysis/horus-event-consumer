package pt.haslab.horus.graph.timeline;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.haslab.horus.Config;

public class TimelineIdGeneratorFactory {
    final static Logger logger = LogManager.getLogger(TimelineIdGeneratorFactory.class);

    public static TimelineIdGenerator get() {
        String timeline = Config.getInstance().getConfig().getString("graph.timeline", "thread");

        switch (timeline) {
            case "thread":
                logger.info("Using thread timelines...");
                return new ThreadTimelineIdGenerator();

            case "process":
                logger.info("Using process timelines...");
                return new ProcessTimelineIdGenerator();


            default:
                throw new IllegalArgumentException("Unsupported timeline [" + timeline + "].");
        }
    }
}

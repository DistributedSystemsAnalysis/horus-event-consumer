package pt.haslab.horus.processing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import pt.haslab.horus.Config;
import pt.haslab.horus.graph.ExecutionGraphFactory;
import pt.haslab.horus.graph.datastores.gremlin.GremlinExecutionGraph;
import pt.haslab.horus.graph.datastores.gremlin.RemoteGremlinExecutionGraph;
import pt.haslab.horus.graph.datastores.neo4j.Neo4jExecutionGraph;

public class EventProcessorFactory {
    final static Logger logger = LogManager.getLogger(EventProcessorFactory.class);

    public static EventProcessor getEventProcessor() {
        String processor = Config.getInstance().getConfig().getString("processor");

        switch (processor) {
            case "sync":
                return new SyncEventProcessor(ExecutionGraphFactory.getGraph());

            case "async":
                return new EventProcessorPipeline(ExecutionGraphFactory.getGraph(), Config.getInstance().getConfig());


            default:
                throw new IllegalArgumentException("Unsupported processor [" + processor + "].");
        }
    }
}

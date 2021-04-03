package pt.haslab.horus.graph.datastores.gremlin;

import org.apache.commons.configuration.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tinkerpop.gremlin.structure.Graph;

public class RemoteGremlinExecutionGraph
        extends GremlinExecutionGraph
        implements AutoCloseable {

    private final static Logger logger = LogManager.getLogger(GremlinExecutionGraph.class);

    public RemoteGremlinExecutionGraph(Graph graph, Configuration configuration) {
        super(graph);

        this.graphTraversal = graph.traversal().withRemote(configuration);

        if (logger.isDebugEnabled())
            logger.debug("Current amount of recorded events: " + this.countEvents());
    }

    @Override
    public Graph getGraph() {
        return this.graphTraversal.getGraph();
    }

    public void close()
            throws Exception {
        this.graphTraversal.close();
    }
}

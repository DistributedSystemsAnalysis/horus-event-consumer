package pt.haslab.horus.graph;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import pt.haslab.horus.Config;
import pt.haslab.horus.graph.datastores.NoopExecutionGraph;
import pt.haslab.horus.graph.datastores.gremlin.GremlinExecutionGraph;
import pt.haslab.horus.graph.datastores.gremlin.RemoteGremlinExecutionGraph;
import pt.haslab.horus.graph.datastores.neo4j.Neo4jExecutionGraph;

public class ExecutionGraphFactory {
    final static Logger logger = LogManager.getLogger(ExecutionGraphFactory.class);

    public static ExecutionGraph getGraph() {
        String graphDriver = Config.getInstance().getConfig().getString("graph.driver");

        switch (graphDriver) {
            case "neo4j":
                return createNeo4jGraph();

            case "gremlin":
                return createGremlinGraph();

            case "none":
                return createNoOpGraph();

            default:
                throw new IllegalArgumentException("Unsupported graph driver [" + graphDriver + "].");
        }
    }

    private static ExecutionGraph createNeo4jGraph() {
        logger.info("Setting up a Neo4j graph...");
        String uri = Config.getInstance().getConfig().getString("neo4j.uri");
        String user = Config.getInstance().getConfig().getString("neo4j.user");
        String password = Config.getInstance().getConfig().getString("neo4j.password");

        AuthToken authToken = user == null ? AuthTokens.none() : AuthTokens.basic(user, password);
        Driver neo4jDriver = GraphDatabase.driver(uri, authToken);

        return new Neo4jExecutionGraph(neo4jDriver);
    }

    private static ExecutionGraph createGremlinGraph() {
        String graphMode = Config.getInstance().getConfig().getString("gremlin.mode");

        Graph localGraph = org.apache.tinkerpop.gremlin.structure.util.GraphFactory.open(
                Config.getInstance().getConfig());

        switch (graphMode) {
            case "embedded":
                logger.info("Setting up a Gremlin-based embedded graph...");
                return new GremlinExecutionGraph(localGraph);

            case "remote":
                logger.info("Setting up a Gremlin-based remote graph with properties...");

                return new RemoteGremlinExecutionGraph(localGraph, Config.getInstance().getConfig());

            default:
                throw new IllegalArgumentException("Unsupported Gremlin graph mode [" + graphMode + "].");

        }
    }

    private static ExecutionGraph createNoOpGraph() {
        return new NoopExecutionGraph();
    }
}

package kr.kdelab.neo4jalt;

import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PathExpanderBuilder;
import org.neo4j.logging.Log;

public class Dummy {
    public static double getCostByVirtual(Log log, Node startNode, Node goalNode) {
        PathFinder<WeightedPath> algo = GraphAlgoFactory.dijkstra(
                PathExpanderBuilder.allTypes(Direction.OUTGOING).build(),
                "weight"
        );
        WeightedPath weightedPath = algo.findSinglePath(startNode, goalNode);
        if (weightedPath == null) {
//            not connected -_-;;
            return Double.MAX_VALUE;
        }

        return weightedPath.weight();
    }
}

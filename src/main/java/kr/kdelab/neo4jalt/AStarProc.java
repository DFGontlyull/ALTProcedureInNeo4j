//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package kr.kdelab.neo4jalt;

import com.carrotsearch.hppc.*;
import com.carrotsearch.hppc.cursors.IntDoubleCursor;
import kr.kdelab.neo4jalt.model.IndexingData;
import kr.kdelab.neo4jalt.model.IndexingResult;
import org.neo4j.graphalgo.CommonEvaluators;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.SCHEMA;

//import kr.kdelab.neo4jalt.AStarProc.TestObj;

public class AStarProc {
    @Context
    public GraphDatabaseAPI api;
    @Context
    public GraphDatabaseService db;
    @Context
    public Log log;
    @Context
    public KernelTransaction transaction;

    public AStarProc() {
    }

    public static class TestObj {
        public String value;
    }

    @Procedure(name = "kde.test", mode = SCHEMA)
    public Stream<TestObj> test(@Name("nodeId") long nodeId) {
        Node node = db.getNodeById(nodeId);

        TestObj testObj = new TestObj();
        testObj.value = (String) node.getProperty("name");
        List<TestObj> list = new LinkedList<>();
        list.add(testObj);

        return list.stream();
    }

    @Procedure(
            name = "kde.astar",
            mode = Mode.SCHEMA
    )
    public Stream<WeightedPathResult> astar(@Name("startNode") Node startNode, @Name("endNode") Node endNode) throws SQLException {
        Connection connection = DatabaseUtil.getConnection();

        try {
            connection = DatabaseUtil.getConnection();
            LongArrayList landmarks = DatabaseUtil.getLandmarks(connection);
            Stream var6 = WeightedPathResult.streamWeightedPathResult(startNode, endNode, GraphAlgoFactory.aStar(PathExpanderBuilder.allTypes(Direction.OUTGOING).build(), CommonEvaluators.doubleCostEvaluator("weight"), (u, t) -> {
                double maxEstimate = 0.0D;
                return maxEstimate;
            }));
            return var6;
        } catch (SQLException var10) {
            this.log.error(Util.toString(var10));
        } finally {
            connection.close();
        }

        return null;
    }

    @Procedure(
            name = "kde.dijkstra",
            mode = Mode.SCHEMA
    )
    public Stream<WeightedPathResult> dijkstra(@Name("startNode") Node startNode, @Name("endNode") Node endNode) throws SQLException {
        return WeightedPathResult.streamWeightedPathResult(startNode, endNode, GraphAlgoFactory.dijkstra(PathExpanderBuilder.allTypes(Direction.OUTGOING).build(), CommonEvaluators.doubleCostEvaluator("weight")));
    }

    private long randomSelect(long rowSize) {
        Random random = new Random();
        int skipValue = random.nextInt((int)rowSize);
        Result result = this.db.execute("MATCH (n) RETURN id(n) SKIP " + skipValue + " LIMIT 1");
        Throwable var8 = null;

        long ret;
        try {
            ret = Long.parseLong(String.valueOf(result.next().values().iterator().next()));
        } catch (Throwable var17) {
            var8 = var17;
            throw var17;
        } finally {
            if (result != null) {
                if (var8 != null) {
                    try {
                        result.close();
                    } catch (Throwable var16) {
                        var8.addSuppressed(var16);
                    }
                } else {
                    result.close();
                }
            }

        }

        return ret;
    }

    private long deepSelect(Graph graph, long tagetNodeId) {
        int count = 0;
        LongHashSet visited = new LongHashSet();
        long currentNodeId = tagetNodeId;

        while(true) {
            AtomicInteger nodeCount = new AtomicInteger();
            AtomicInteger lastNodeId = new AtomicInteger();
            graph.forEachIncoming(graph.toMappedNodeId(currentNodeId), (sourceNodeId, targetNodeId, relationId) -> {
                nodeCount.getAndIncrement();
                lastNodeId.set(targetNodeId);
                return true;
            });
            if (nodeCount.get() != 1) {
                break;
            }

            currentNodeId = graph.toOriginalNodeId(lastNodeId.get());
            if (visited.contains(currentNodeId)) {
                break;
            }

            visited.add(currentNodeId);
        }

        return currentNodeId;
    }

    private ObjectArrayList<IndexingData> expand(Graph graph, long id, int areaId, boolean initialize, LongArrayDeque isolatedNodes) {
        boolean loop = false;
        ShortestPaths algo = ((ShortestPaths)((ShortestPaths)(new ShortestPaths(graph)).withProgressLogger(ProgressLogger.wrap(this.log, "ShortestPaths"))).withTerminationFlag(TerminationFlag.wrap(this.transaction))).withDirection(Direction.OUTGOING).compute(id);
        ObjectArrayList<IndexingData> ret = new ObjectArrayList();
        IntDoubleScatterMap costs = (IntDoubleScatterMap)algo.getShortestPaths();
        Iterator var11 = costs.iterator();

        IntDoubleCursor intCursor;
        long start;
        Double cost;
        while(var11.hasNext()) {
            intCursor = (IntDoubleCursor)var11.next();
            start = graph.toOriginalNodeId(intCursor.key);
            if (start == id) {
                loop = true;
            }

            cost = intCursor.value;
            if (Double.isInfinite(cost)) {
                if (initialize) {
                    ;
                }
            } else {
                ret.add(new IndexingData(id, start, cost, areaId));
            }
        }

        algo.release();
        costs.release();
        algo = ((ShortestPaths)((ShortestPaths)(new ShortestPaths(graph)).withProgressLogger(ProgressLogger.wrap(this.log, "ShortestPaths"))).withTerminationFlag(TerminationFlag.wrap(this.transaction))).withDirection(Direction.INCOMING).compute(id);
        costs = (IntDoubleScatterMap)algo.getShortestPaths();
        var11 = costs.iterator();

        while(var11.hasNext()) {
            intCursor = (IntDoubleCursor)var11.next();
            start = graph.toOriginalNodeId(intCursor.key);
            if (start == id) {
                loop = true;
            }

            cost = intCursor.value;
            if (Double.isInfinite(cost)) {
                if (initialize) {
                    ;
                }
            } else {
                ret.add(new IndexingData(start, id, cost, areaId));
            }
        }

        if (!loop) {
            ret.add(new IndexingData(id, id, 0.0D, areaId));
        }

        costs.release();
        algo.release();
        return ret;
    }

    private IntDoubleScatterMap expandOutgoing(Graph graph, long id) {
        ShortestPaths algo = ((ShortestPaths)((ShortestPaths)(new ShortestPaths(graph)).withProgressLogger(ProgressLogger.wrap(this.log, "ShortestPaths"))).withTerminationFlag(TerminationFlag.wrap(this.transaction))).withDirection(Direction.OUTGOING).compute(id);
        IntDoubleScatterMap costs = (IntDoubleScatterMap)algo.getShortestPaths();
        algo.release();
        return costs;
    }

    @Procedure(
            name = "kde.astar.indexing",
            mode = Mode.SCHEMA
    )
    public Stream<IndexingResult> indexing(@Name(value = "size",defaultValue = "3") long indexSize) throws SQLException {
        try {
            Connection connection = DatabaseUtil.getConnection();
            LongArrayDeque isolatedNodes = new LongArrayDeque();
            Result result = this.db.execute("MATCH (n) RETURN count(*)");
            Throwable var8 = null;

            long rowSize;
            try {
                rowSize = Long.parseLong(String.valueOf(result.next().values().iterator().next()));
            } catch (Throwable var19) {
                var8 = var19;
                throw var19;
            } finally {
                if (result != null) {
                    if (var8 != null) {
                        try {
                            result.close();
                        } catch (Throwable var18) {
                            var8.addSuppressed(var18);
                        }
                    } else {
                        result.close();
                    }
                }

            }

            if (rowSize == 0L) {
                return null;
            } else {
                ProcedureConfiguration configuration = ProcedureConfiguration.create(new TreeMap());
                Graph graph = (new GraphLoader(this.api, Pools.DEFAULT)).init(this.log, configuration.getNodeLabelOrQuery(), configuration.getRelationshipOrQuery(), configuration).withOptionalRelationshipWeightsFromProperty("weight", configuration.getWeightPropertyDefaultValue(1.0D)).withDirection(Direction.BOTH).load(configuration.getGraphImpl());
                int areaId = 0;

                for(int i = 0; (long)i < indexSize; ++i) {
                    long tmp = this.randomSelect(rowSize);
                    DatabaseUtil.addBulkData(connection, tmp, this.expand(graph, tmp, areaId, true, isolatedNodes));
                }

                ArrayList<IndexingResult> indexingResults = DatabaseUtil.getIndexingResults(connection);
                DatabaseUtil.close(connection);
                graph.release();
                return indexingResults.stream();
            }
        } catch (Exception var21) {
            this.log.error(Util.toString(var21));
            return null;
        }
    }

    @Procedure(
            name = "kde.astar.indexing.outgoing",
            mode = Mode.SCHEMA
    )
    public Stream<IndexingResult> indexingOutgoing(@Name(value = "size",defaultValue = "3") long indexSize) throws SQLException {
        try {
            Connection connection = DatabaseUtil.getConnection();
            Result result = this.db.execute("MATCH (n) RETURN count(*)");
            Throwable var7 = null;

            long rowSize;
            try {
                rowSize = Long.parseLong(String.valueOf(result.next().values().iterator().next()));
            } catch (Throwable var17) {
                var7 = var17;
                throw var17;
            } finally {
                if (result != null) {
                    if (var7 != null) {
                        try {
                            result.close();
                        } catch (Throwable var16) {
                            var7.addSuppressed(var16);
                        }
                    } else {
                        result.close();
                    }
                }

            }

            if (rowSize == 0L) {
                return null;
            } else {
                ProcedureConfiguration configuration = ProcedureConfiguration.create(new TreeMap());
                Graph graph = (new GraphLoader(this.api, Pools.DEFAULT)).init(this.log, configuration.getNodeLabelOrQuery(), configuration.getRelationshipOrQuery(), configuration).withOptionalRelationshipWeightsFromProperty("weight", configuration.getWeightPropertyDefaultValue(1.0D)).withDirection(Direction.OUTGOING).load(configuration.getGraphImpl());

                for(int i = 0; (long)i < indexSize; ++i) {
                    long tmp = this.randomSelect(rowSize);
                    DatabaseUtil.addBulkData(connection, tmp, graph, this.expandOutgoing(graph, tmp));
                }

                ArrayList<IndexingResult> indexingResults = DatabaseUtil.getIndexingResults(connection);
                DatabaseUtil.close(connection);
                graph.release();
                return indexingResults.stream();
            }
        } catch (Exception var19) {
            this.log.error(Util.toString(var19));
            return null;
        }
    }
}

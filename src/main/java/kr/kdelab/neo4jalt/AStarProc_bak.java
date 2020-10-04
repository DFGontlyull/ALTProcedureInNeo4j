//package kr.kdelab.neo4jalt;
//
//import com.carrotsearch.hppc.*;
//import com.carrotsearch.hppc.cursors.IntDoubleCursor;
//import com.carrotsearch.hppc.cursors.LongCursor;
//import kr.kdelab.neo4jalt.model.IndexingData;
//import kr.kdelab.neo4jalt.model.IndexingResult;
////import kr.kdelab.neo4jalt.redisdb.JedisHelper;
////import kr.kdelab.neo4jalt.redisdb.RedisUtil;
//import org.neo4j.graphalgo.CommonEvaluators;
//import org.neo4j.graphalgo.GraphAlgoFactory;
//import org.neo4j.graphalgo.api.Graph;
//import org.neo4j.graphalgo.core.GraphLoader;
//import org.neo4j.graphalgo.core.ProcedureConfiguration;
//import org.neo4j.graphalgo.core.utils.Pools;
//import org.neo4j.graphalgo.core.utils.ProgressLogger;
//import org.neo4j.graphalgo.core.utils.TerminationFlag;
//import org.neo4j.graphdb.*;
//import org.neo4j.internal.kernel.api.Session;
//import org.neo4j.kernel.api.KernelTransaction;
//import org.neo4j.kernel.internal.GraphDatabaseAPI;
//import org.neo4j.logging.Log;
//import org.neo4j.procedure.Context;
//import org.neo4j.procedure.Name;
//import org.neo4j.procedure.Procedure;
////import redis.clients.jedis.Jedis;
//
//import java.sql.SQLException;
//import java.util.*;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.stream.Stream;
//
//import static org.neo4j.procedure.Mode.SCHEMA;
//
//public class AStarProc_bak {
//
//    @Context
//    public GraphDatabaseAPI api;
//
//    @Context
//    public GraphDatabaseService db;
//
//    @Context
//    public Log log;
//
//    @Context
//    public KernelTransaction transaction;
//
//    @Procedure(name = "kde.test", mode = SCHEMA)
//    public Stream<AStarProc_bak.TestObj> test(@Name("nodeId") long nodeId) {
//        Node node = db.getNodeById(nodeId);
//
//        AStarProc_bak.TestObj testObj = new AStarProc_bak.TestObj();
//        testObj.value = (String) node.getProperty("name");
//        List<AStarProc_bak.TestObj> list = new LinkedList<>();
//        list.add(testObj);
//
//        return list.stream();
//    }
////    @Procedure(name = "kde.inputdata", mode = SCHEMA)
////    public Stream<tempNode> inputdata(@Name("nodeId") long nodeId,
////                                      @Name("startnum") int start,
////                                      @Name("endnum") int end) throws SQLException {
////        Connection connection = DatabaseUtil.getConnection_sample();
////        try {
////            connection = DatabaseUtil.getConnection_sample();
////            Connection finalConnection = connection;
////
////        } catch (SQLException e) {
////            log.error(Util.toString(e));
////        } finally {
////            connection.close();
////        }
////        return null;
////    }
//
//
////    @Procedure(name = "kde.astar", mode = SCHEMA)
////    public Stream<WeightedPathResult> astar(@Name("startNode") Node startNode,
////                                            @Name("endNode") Node endNode) throws SQLException {
////        JedisHelper jhelp = JedisHelper.getInstance();
////        try {
////            Jedis jedis = RedisUtil.getConnection(jhelp);
////            LongArrayList landmarks = RedisUtil.getLandmarks(jedis);
////            Jedis finalConnection = jedis;
////            return WeightedPathResult.streamWeightedPathResult(startNode, endNode, GraphAlgoFactory.aStar(PathExpanderBuilder.allTypes(Direction.OUTGOING).build(), CommonEvaluators.doubleCostEvaluator("weight"), (u, t) -> {
////                double maxEstimate = 0d;
////                if (Double.isFinite(maxEstimate = RedisUtil.getDirectedIndex(finalConnection, u.getId(), t.getId()))) {
////                    return maxEstimate;
////                }
////                maxEstimate = 0d;
////
//////                    long s = System.currentTimeMillis();
////
////                int landmarkCount = 0;
////                for (LongCursor landmark : landmarks) {
////                    double estimate;
////                    estimate = RedisUtil.calcEstimate(finalConnection, u.getId(), t.getId(), landmark.value);
////
////                    if (Double.isFinite(estimate)) {
////                        maxEstimate = Math.max(maxEstimate, estimate);
////                    }
////
////                    if (landmarkCount++ > 5)
////                        break;
////                }
//////                    log.error(System.currentTimeMillis() - s + "t");
////                return maxEstimate;
////            }));
////        } finally {
////            RedisUtil.closeConnection(jhelp);
////        }
////    }
//
////    public List<String> getPeople()
////    {
////        try ( Session session = .session() )
////        {
////            return session.readTransaction( new TransactionWork<List<String>>()
////            {
////                @Override
////                public List<String> execute( Transaction tx )
////                {
////                    return matchPersonNodes( tx );
////                }
////            } );
////        }
////    }
////
////    private static List<String> matchPersonNodes( Transaction tx )
////    {
////        List<String> names = new ArrayList<>();
////        Random random = new Random();
////        int skipValue = random.nextInt((int) 5);
////        StatementResult result = tx.run("MATCH (n) RETURN id(n) SKIP " + skipValue + " LIMIT 1");
////        while ( result.hasNext() )
////        {
////            names.add( result.next().get( 0 ).asString() );
////        }
////        return names;
////    }
//
//    private long randomSelect(long rowSize) {
//        long ret;
//        Random random = new Random();
//        int skipValue = random.nextInt((int) rowSize);
//        try (Result result = db.execute("MATCH (n) RETURN id(n) SKIP " + skipValue + " LIMIT 1")) {
//            ret = Long.parseLong(String.valueOf(result.next().values().iterator().next()));
//        }
//        return ret;
//    }
//
//    private long deepSelect(Graph graph, long tagetNodeId) {
//        int count = 0;
//        LongHashSet visited = new LongHashSet();
//        long currentNodeId = tagetNodeId;
//        while (true) {
//            AtomicInteger nodeCount = new AtomicInteger();
//            AtomicInteger lastNodeId = new AtomicInteger();
//
//            graph.forEachIncoming(graph.toMappedNodeId(currentNodeId), (sourceNodeId, targetNodeId, relationId) -> {
//                nodeCount.getAndIncrement();
//                lastNodeId.set(targetNodeId);
//                return true;
//            });
//
//            if (nodeCount.get() == 1) {
//                currentNodeId = graph.toOriginalNodeId(lastNodeId.get());
//                if (visited.contains(currentNodeId))
//                    break;
//                visited.add(currentNodeId);
////                log.info("딥셀렉. " + (count++) + "- " + currentNodeId);
//            } else break;
//        }
//        return currentNodeId;
//    }
//
//    private ObjectArrayList<IndexingData> expand(Graph graph, long id, int areaId, boolean initialize, LongArrayDeque isolatedNodes) {
//        boolean loop = false;
//        ShortestPaths algo = new ShortestPaths(graph)
//                .withProgressLogger(ProgressLogger.wrap(log, "ShortestPaths"))
//                .withTerminationFlag(TerminationFlag.wrap(transaction))
//                .withDirection(Direction.OUTGOING)
//                .compute(id);
//        ObjectArrayList<IndexingData> ret = new ObjectArrayList<>();
//        IntDoubleScatterMap costs = (IntDoubleScatterMap) algo.getShortestPaths();
//        for (IntDoubleCursor intCursor : costs) {
//            long goal = graph.toOriginalNodeId(intCursor.key);
//            if (goal == id)
//                loop = true;
//
//            Double cost = intCursor.value;
//
//            if (Double.isInfinite(cost)) {
//                if (initialize) {
////                    isolatedNodes.addLast(goal);
////                    log.error("ISOLATED " + id + " " + goal + " " + areaId);
//                }
//            } else {
////                isolatedNodes.remove(goal);
//                ret.add(new IndexingData(id, goal, cost, areaId));
//            }
//        }
//        // OUTGOING A -> B,C,D 를 계산
//
//        algo.release();
//        costs.release();
//
//
//        algo = new ShortestPaths(graph)
//                .withProgressLogger(ProgressLogger.wrap(log, "ShortestPaths"))
//                .withTerminationFlag(TerminationFlag.wrap(transaction))
//                .withDirection(Direction.INCOMING)
//                .compute(id);
//        costs = (IntDoubleScatterMap) algo.getShortestPaths();
//        for (IntDoubleCursor intCursor : costs) {
//            long start = graph.toOriginalNodeId(intCursor.key);
//            if (start == id)
//                loop = true;
//
//            Double cost = intCursor.value;
//
//            if (Double.isInfinite(cost)) {
//                if (initialize) {
////                    isolatedNodes.addLast(start);
////                    log.error("ISOLATED " + id + " " + start + " " + areaId);
//                }
//            } else {
////                isolatedNodes.remove(start);
//                ret.add(new IndexingData(start, id, cost, areaId));
//            }
//        }
//        // INCOMING B,C,D -> A를 계산
//
//
//        if (!loop) {
//            ret.add(new IndexingData(id, id, 0.0, areaId));
////            isolatedNodes.remove(id);
//        }
//
//        costs.release();
//        algo.release();
//
//        return ret;
//    }
//
//    private IntDoubleScatterMap expandOutgoing(Graph graph, long id) {
//        ShortestPaths algo = new ShortestPaths(graph)
//                .withProgressLogger(ProgressLogger.wrap(log, "ShortestPaths"))
//                .withTerminationFlag(TerminationFlag.wrap(transaction))
//                .withDirection(Direction.OUTGOING)
//                .compute(id);
////        ObjectArrayList<IndexingData> ret = new ObjectArrayList<>();
//        IntDoubleScatterMap costs = (IntDoubleScatterMap) algo.getShortestPaths();
//
//        algo.release();
//
//        return costs;
//    }
//
//    Session sess;
////    @Procedure(name = "kde.astar.indexing", mode = SCHEMA)
////    public Stream<IndexingResult> indexing(@Name(value = "size", defaultValue = "3") long indexSize) throws SQLException {
////        try {
////            JedisHelper jhelp = JedisHelper.getInstance();
////            Jedis jedis = RedisUtil.getConnection(jhelp);
//////            LongHashSet isolatedNodes = new LongHashSet();
////            LongArrayDeque isolatedNodes = new LongArrayDeque();
////
////            long rowSize;
////            try (Result result = db.execute("MATCH (n) RETURN count(*)")) {
////                rowSize = Long.parseLong(String.valueOf(result.next().values().iterator().next()));
////            }
////
//////        NODE가 존재하지 않는 경우
////            if (rowSize == 0)
////                return null;
////
////            ProcedureConfiguration configuration = ProcedureConfiguration.create(new TreeMap<>());
////
////            final Graph graph = new GraphLoader(api, Pools.DEFAULT)
////                    .init(log, configuration.getNodeLabelOrQuery(), configuration.getRelationshipOrQuery(), configuration)
////                    .withOptionalRelationshipWeightsFromProperty(
////                            "weight",
////                            configuration.getWeightPropertyDefaultValue(1.0))
////                    .withDirection(Direction.BOTH)
////                    .load(configuration.getGraphImpl());
////
////// OUTGOING 노드 -> 모든 노드
////// INGOING 모든 노드 -> 노드
//////            LongHashSet visitedNodes = new LongHashSet();
////            int areaId = 0;
//////            long initialNode = deepSelect(graph, );
//////            log.info("초기 노드 = " + initialNode);
////            for (int i = 0; i < indexSize; i++) {
////                long tmp = randomSelect(rowSize);
////                RedisUtil.addBulkData(jedis, tmp, expand(graph, tmp, areaId, true, isolatedNodes));
////
////            }
//////            DatabaseUtil.addBulkData(connection, initialNode, expand(graph, randomSelect(rowSize), areaId, true, isolatedNodes));
////
//////            DatabaseUtil.addBulkData(connection, initialNode, expand(graph, randomSelect(rowSize), areaId, true, isolatedNodes));
//////
//////            while (!isolatedNodes.isEmpty()) {
//////                long iNode = isolatedNodes.getFirst();
////////                log.info("고립 노드 = " + iNode);
//////                long deepId = deepSelect(graph, iNode);
//////                if (visitedNodes.contains(deepId)) {
//////                    isolatedNodes.removeFirst();
//////                    continue;
//////                } else
//////                    areaId++;
//////
//////                DatabaseUtil.addBulkData(connection, deepId, expand(graph, deepId, areaId, false, isolatedNodes));
//////                visitedNodes.add(deepId);
//////                isolatedNodes.removeFirst();
//////            }
////
////            ArrayList<IndexingResult> indexingResults = RedisUtil.getIndexingResults(jedis);
////
////            jhelp.destoryPool();
////            graph.release();
////
////            return indexingResults.stream();
////        } catch (Exception e) {
////            log.error(Util.toString(e));
////        }
////        return null;
////    }
//
////    @Procedure(name = "kde.astar.indexing.outgoing")
////    public Stream<IndexingResult> indexingOutgoing(@Name(value = "size", defaultValue = "3") long indexSize) throws SQLException {
////        try {
////            JedisHelper jhelp = JedisHelper.getInstance();
////            Jedis jedis = RedisUtil.getConnection(jhelp);
////            long rowSize;
////            try (Result result = db.execute("MATCH (n) RETURN count(*)")) {
////                rowSize = Long.parseLong(String.valueOf(result.next().values().iterator().next()));
////            }
////
//////        NODE가 존재하지 않는 경우
////            if (rowSize == 0)
////                return null;
////
////            ProcedureConfiguration configuration = ProcedureConfiguration.create(new TreeMap<>());
////
////            final Graph graph = new GraphLoader(api, Pools.DEFAULT)
////                    .init(log, configuration.getNodeLabelOrQuery(), configuration.getRelationshipOrQuery(), configuration)
////                    .withOptionalRelationshipWeightsFromProperty(
////                            "weight",
////                            configuration.getWeightPropertyDefaultValue(1.0))
////                    .withDirection(Direction.OUTGOING)
////                    .load(configuration.getGraphImpl());
////
////            for (int i = 0; i < indexSize; i++) {
////                long tmp = randomSelect(rowSize);
////                RedisUtil.addBulkData(jedis, tmp, graph, expandOutgoing(graph, tmp));
////            }
////
////            ArrayList<IndexingResult> indexingResults = RedisUtil.getIndexingResults(jedis);
////            jedis.close();
////            graph.release();
////
////            return indexingResults.stream();
////        } catch (Exception e) {
////            log.error(Util.toString(e));
////        }
////        return null;
////    }
//
//
//    public static class TestObj {
//        public String value;
//    }
//}
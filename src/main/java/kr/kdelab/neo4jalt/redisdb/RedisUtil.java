//package kr.kdelab.neo4jalt.redisdb;
//
//import com.carrotsearch.hppc.IntDoubleScatterMap;
//import com.carrotsearch.hppc.LongArrayList;
//import com.carrotsearch.hppc.ObjectArrayList;
//import com.carrotsearch.hppc.cursors.IntDoubleCursor;
//import com.carrotsearch.hppc.cursors.ObjectCursor;
//import kr.kdelab.neo4jalt.model.IndexingData;
//import kr.kdelab.neo4jalt.model.IndexingResult;
//import org.neo4j.graphalgo.api.Graph;
//import redis.clients.jedis.Jedis;
//import redis.clients.jedis.Pipeline;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Iterator;
//import java.util.SortedSet;
//
//public class RedisUtil {
//    public static void main(String[] args) {
////        temp t1 = new temp(JedisHelper.getInstance());
////        t1.addVisit("home");
////        makeschema(getConnection(JedisHelper.getInstance()));
//    }
//
////    public static void makeschema(Jedis jedis){
////        SortedMap<String, String> result = new TreeMap<String, String>();
////        final String KEY_LANDMARK = "index:landmark:";
////        final String KEY_START = "index:start:";
////        final String KEY_END = "index:end:";
////        final String KEY_WEIGHT = "index:weight:";
////        final String KEY_CULANDMARK = "index:currentlandmark:";
////        result.put(KEY_LANDMARK,"");
////        result.put(KEY_START,"");
////        result.put(KEY_END,"");
////        result.put(KEY_WEIGHT,"");
//
////        jedis.hmset("index:", result);
////        jedis.hmset("index:", result);
////    }
//
//    public static Jedis getConnection(JedisHelper jedisHelper){
//        Jedis jedis = jedisHelper.getConnection();
//        return jedis;
//    }
//
//    public static LongArrayList getLandmarks(Jedis jedis){
//        LongArrayList landmarks = new LongArrayList();
//        SortedSet<String> set = (SortedSet<String>) jedis.zrange("landmark", 0, -1);
//        Iterator<String> it = set.iterator();
//        while(it.hasNext()){
//            landmarks.add(Long.parseLong(it.next()));
//        }
//        return landmarks;
////        Map<String, String> fields = jedis.hgetAll("index:");
////        Set<Map.Entry<String, String>> entries = fields.entrySet();
////        Iterator<Map.Entry<String, String>> i = entries.iterator();
////
////        while(i.hasNext()) {
////            Map.Entry<String, String> entry = i.next();
////            landmarks.add(i.);
////        }
//
////        try (Statement statement = connection.createStatement()) {
////            try (ResultSet rs = statement.executeQuery("SELECT DISTINCT landmark FROM altIndex")) {
////                while (rs.next()) {
////                    landmarks.add(rs.getLong(1));
////                }
////            }
////        }
//    }
//
//    public static double getDirectedIndex(Jedis jedis, long start, long goal){
//        double cost = Double.POSITIVE_INFINITY;
//        Pipeline pipe = jedis.pipelined();
//
//        pipe.hget("index:", "cost");
////        try (PreparedStatement statement = connection.prepareStatement("SELECT cost FROM altIndex WHERE start=? and goal=?")) {
////            statement.setLong(1, start);
////            statement.setLong(2, goal);
////
////            try (ResultSet rs = statement.executeQuery()) {
////                if (rs.next()) {
////                    cost = rs.getDouble(1);
////                }
////            }
////        }
//        return cost;
//    }
//
//    public static double calcEstimate(Jedis jedis, long u, long t, long landmark) {
////                            estimate = Math.max(DatabaseUtil.getDirectedIndex(connection, u.getId(), landmark.value) - DatabaseUtil.getDirectedIndex(connection, t.getId(), landmark.value),
////                            DatabaseUtil.getDirectedIndex(connection, landmark.value, t.getId()) - DatabaseUtil.getDirectedIndex(connection, landmark.value, u.getId()));
//        double[] ret = {0d, 0d};
//
//
////        try (PreparedStatement statement = connection.prepareStatement("SELECT a.cost-b.cost FROM (SELECT cost FROM altIndex WHERE start=? and goal=?) as a, (SELECT cost FROM altIndex WHERE start=? and goal=?) as b")) {
////            statement.setLong(1, u);
////            statement.setLong(2, landmark);
////            statement.setLong(3, t);
////            statement.setLong(4, landmark);
////
////            try (ResultSet rs = statement.executeQuery()) {
////                if (rs.next()) {
////                    ret[0] = rs.getDouble(1);
////                }
////            }
////        }
//
//        return Math.max(ret[0], ret[1]);
//    }
//
//    public static void closeConnection(JedisHelper jedisHelper){
//        jedisHelper.destoryPool();
////        Jedis jedis = new Jedis(new URI("redis://:foobared@210.119.105.221:6379/home/20133062/redis"));
//    }
//
//    public static void addBulkData(Jedis jedis, long landmarkId, Graph graph, IntDoubleScatterMap indexingData){
//        Pipeline pipeline = jedis.pipelined();
//        Iterator<IntDoubleCursor> iter = indexingData.iterator();
//        int num = 0;
//        for(IntDoubleCursor idxCursor : indexingData){
//            if (Double.isInfinite(idxCursor.value))
//                continue;
//
//            num = Integer.parseInt(jedis.hget("index:", "area"))+1;
//            pipeline.hset("index:"+num, "landmarkId", String.valueOf(landmarkId));
//            pipeline.hset("index:"+num, "start", String.valueOf(graph.toOriginalNodeId(idxCursor.key)));
//            pipeline.hset("index:"+num, "end", String.valueOf(idxCursor.value));
//            pipeline.hincrBy("index:"+num,"area",1);
//            pipeline.hset("index:"+num, "currentlandmark", String.valueOf(landmarkId));
////            pipeline.hset("index:"+num, "cost", );
//            pipeline.zadd("landmark", landmarkId, String.valueOf(landmarkId));
//            pipeline.zadd("goal", landmarkId, String.valueOf(landmarkId));
////            pipeline.hget("index:weight:", String.valueOf(landmarkId));
//        }
//        pipeline.exec();
//
////        while (iter.hasNext()) {
////            String[] keyValue = iter.next().split("\t");
////            pipeline.sadd(keyValue[0], keyValue[1]);
////            // you can call pipeline.sync() and start new pipeline here if you think there're so much operations in one pipeline
////        }
////        pipeline.sync();
//}
//    public static void addBulkData(Jedis jedis, long landmarkId, ObjectArrayList<IndexingData> indexingData) throws IOException, IOException {
//        final int batchSize = 10000;
//        int count = 0;
//        int num = 0;
//
//        Pipeline pipeline = jedis.pipelined();
//        Iterator<ObjectCursor<IndexingData>> iter = indexingData.iterator();
//        for (ObjectCursor<IndexingData> idxCursor : indexingData) {
//            IndexingData target = idxCursor.value;
//            if (target.cost == null) continue;
//
//            num = Integer.parseInt(jedis.hget("index:", "area"))+1;
//            pipeline.hset("index:"+num, "landmarkId", String.valueOf(landmarkId));
//            pipeline.hset("index:"+num, "start", String.valueOf(target.start));
//            pipeline.hset("index:"+num, "end", String.valueOf(target.end));
//            pipeline.hset("index:"+num, "cost", String.valueOf(target.cost));
//            pipeline.hset("index:"+num, "area", String.valueOf(target.areaId));
//            pipeline.hset("index:"+num, "currentlandmark", String.valueOf(landmarkId));
////            pipeline.hget("index:weight:", String.valueOf(landmarkId));
//
//            if (++count % batchSize == 0) {
//                pipeline.exec();
//            }
//        }
//        pipeline.exec();
//        pipeline.close();
//    }
//
//    public static ArrayList<IndexingResult> getIndexingResults(Jedis jedis){
//        ArrayList<IndexingResult> arrayList = new ArrayList<>();
//        arrayList.add(new IndexingResult(0, (int) jedis.pfcount("goal")));
////        try (Statement statement = connection.createStatement()) {
////            try (ResultSet resultSet = statement.executeQuery("SELECT DISTINCT area, COUNT(goal) FROM altIndex GROUP BY area")) {
////                while (resultSet.next()) {
////                    arrayList.add(new IndexingResult(resultSet.getInt("area"), resultSet.getInt(2)));
////                }
////            }
////        }
//        return arrayList;
//    }
//}
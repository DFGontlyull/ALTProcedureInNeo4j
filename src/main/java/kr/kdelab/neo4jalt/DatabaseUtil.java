package kr.kdelab.neo4jalt;

import com.carrotsearch.hppc.IntDoubleScatterMap;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.ObjectArrayList;
import com.carrotsearch.hppc.cursors.IntDoubleCursor;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import kr.kdelab.neo4jalt.model.IndexingData;
import kr.kdelab.neo4jalt.model.IndexingResult;
import org.neo4j.graphalgo.api.Graph;

import java.sql.*;
import java.util.ArrayList;

public class DatabaseUtil {
    public static ArrayList<IndexingResult> getIndexingResults(Connection connection) throws SQLException {
        ArrayList<IndexingResult> arrayList = new ArrayList<>();
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery("SELECT DISTINCT area, COUNT(goal) FROM altIndex GROUP BY area")) {
                while (resultSet.next()) {
                    arrayList.add(new IndexingResult(resultSet.getInt("area"), resultSet.getInt(2)));
                }
            }
        }
        return arrayList;
    }

    public static Connection getConnection() throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:sqlite:/tmp/neo4j/alt-index.db");
//        jdbc:sqlite:alt-index.db
        createScheme(connection);
        return connection;
    }

//    public static Connection getConnection_sample() throws  SQLException{
//        Connection connection = DriverManager.getConnection("jdbc:sqlite:/tmp/neo4j/sampledata.db");
//        createScheme(connection);
//        return connection;
//    }

//    private static void createScheme_sample(Connection connection) throws SQLException {
//        Statement statement = connection.createStatement();
//        statement.executeUpdate("CREATE TABLE IF NOT EXISTS altIndex (start INTEGER, goal INTEGER, cost REAL);");
//        statement.close();
//    }
//converted
    public static void addBulkData(Connection connection, long landmarkId, ObjectArrayList<IndexingData> indexingData) throws SQLException {
        final int batchSize = 10000;
        int count = 0;

        String sql = "INSERT OR IGNORE INTO altIndex VALUES (?, ?, ?, ?, ?)";


        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (ObjectCursor<IndexingData> idxCursor : indexingData) {
                IndexingData target = idxCursor.value;
                if (target.cost == null) continue;

                statement.setLong(1, target.start);
                statement.setLong(2, target.end);
                statement.setDouble(3, target.cost);
                statement.setInt(4, target.areaId);
                statement.setLong(5, landmarkId);
//                statement.setInt(6, direction == Direction.OUTGOING ? 1 : 0);
                statement.addBatch();

                if (++count % batchSize == 0) {
                    statement.executeBatch();
                    connection.commit();
                }
            }
            statement.executeBatch();
            connection.commit();
        }
        connection.setAutoCommit(true);
    }

    //converted
    public static void addBulkData(Connection connection, long landmarkId, Graph graph, IntDoubleScatterMap indexingData) throws SQLException {
        connection.setAutoCommit(false);
        final int batchSize = 10000;
        int count = 0;

        String sql = "INSERT OR IGNORE INTO altIndex VALUES (?, ?, ?, ?, ?)";


        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (IntDoubleCursor idxCursor : indexingData) {
//                IndexingData target = idxCursor.value;
//                if (target.cost == null) continue;
                if (Double.isInfinite(idxCursor.value))
                    continue;


                statement.setLong(1, landmarkId);
                statement.setLong(2, graph.toOriginalNodeId(idxCursor.key));
                statement.setDouble(3, idxCursor.value);
                statement.setInt(4, 0);
                statement.setLong(5, landmarkId);
//                statement.setInt(6, direction == Direction.OUTGOING ? 1 : 0);
                statement.addBatch();

                if (++count % batchSize == 0) {
                    statement.executeBatch();
                    connection.commit();
                }
            }
            statement.executeBatch();
            connection.commit();
        }
        connection.setAutoCommit(true);
    }

    public static double calcEstimate(Connection connection, long u, long t, long landmark) throws SQLException {
//                            estimate = Math.max(DatabaseUtil.getDirectedIndex(connection, u.getId(), landmark.value) - DatabaseUtil.getDirectedIndex(connection, t.getId(), landmark.value),
//                            DatabaseUtil.getDirectedIndex(connection, landmark.value, t.getId()) - DatabaseUtil.getDirectedIndex(connection, landmark.value, u.getId()));
        double[] ret = {0d, 0d};
        //INGOING
        try (PreparedStatement statement = connection.prepareStatement("SELECT a.cost-b.cost FROM (SELECT cost FROM altIndex WHERE start=? and goal=?) as a, (SELECT cost FROM altIndex WHERE start=? and goal=?) as b")) {
            statement.setLong(1, u);
            statement.setLong(2, landmark);
            statement.setLong(3, t);
            statement.setLong(4, landmark);

            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    ret[0] = rs.getDouble(1);
                }
            }
        }
        //OUTGOING
        try (PreparedStatement statement = connection.prepareStatement("SELECT a.cost-b.cost FROM (SELECT cost FROM altIndex WHERE start=? and goal=?) as a, (SELECT cost FROM altIndex WHERE start=? and goal=?) as b")) {
            statement.setLong(1, landmark);
            statement.setLong(2, t);
            statement.setLong(3, landmark);
            statement.setLong(4, u);

            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    ret[1] = rs.getDouble(1);
                }
            }
        }
        return Math.max(ret[0], ret[1]);
    }

    public static double getDirectedIndex(Connection connection, long start, long goal) throws SQLException {
        double cost = Double.POSITIVE_INFINITY;
        try (PreparedStatement statement = connection.prepareStatement("SELECT cost FROM altIndex WHERE start=? and goal=?")) {
            statement.setLong(1, start);
            statement.setLong(2, goal);

            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    cost = rs.getDouble(1);
                }
            }
        }
        return cost;
    }
//converted
    public static LongArrayList getLandmarks(Connection connection) throws SQLException {
        LongArrayList landmarks = new LongArrayList();
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery("SELECT DISTINCT landmark FROM altIndex")) {
                while (rs.next()) {
                    landmarks.add(rs.getLong(1));
                }
            }
        }
        return landmarks;
    }

//    public static double getIndexCost(Connection connection, Direction direction, long landmarkId, long target) throws SQLException {
//        double indexingData = Double.MAX_VALUE;
//        String query = "SELECT landmark, cost FROM altIndex WHERE landmark=? and goal=? limit 1";
//        if (direction == Direction.INCOMING)
//            query = "SELECT landmark, cost FROM altIndex WHERE landmark=? and start=? limit 1";
//
//        try (PreparedStatement statement = connection.prepareStatement(query)) {
//            statement.setLong(1, landmarkId);
//            statement.setLong(2, target);
//            statement.setLong(3, target);
//
//            try (ResultSet rs = statement.executeQuery()) {
//                if (rs.next()) {
//                    indexingData = rs.getDouble(1);
//                }
//            }
//        }
//        return indexingData;
//    }
//
//    public static LandmarkWithCost getIndexCost(Connection connection, Direction direction, long target) throws SQLException {
//        LandmarkWithCost landmarkWithCost = null;
////        double indexingData = Double.MAX_VALUE;
//        String query = "SELECT landmark, cost FROM altIndex WHERE goal=? limit 1";
//        if (direction == Direction.INCOMING)
//            query = "SELECT landmark, cost FROM altIndex WHERE and start=? limit 1";
//
//        try (PreparedStatement statement = connection.prepareStatement(query)) {
////            statement.setLong(1, landmarkId);
//            statement.setLong(1, target);
////            statement.setLong(3, target);
//
//            try (ResultSet rs = statement.executeQuery()) {
//                if (rs.next()) {
//                    landmarkWithCost = new LandmarkWithCost(rs.getLong(1), rs.getDouble(2));
////                    indexingData = rs.getDouble(1);
//                }
//            }
//        }
//        return landmarkWithCost;
//    }

//    static class LandmarkWithCost {
//        public double cost = Double.MAX_VALUE;
//        public long landmarkId;
//
//        public LandmarkWithCost(long landmarkId, double cost) {
//            this.cost = cost;
//            this.landmarkId = landmarkId;
//        }
//    }

    private static void createScheme(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        statement.executeUpdate("CREATE TABLE IF NOT EXISTS altIndex (start INTEGER, goal INTEGER, cost REAL, area INTEGER, landmark INTEGER);");
        statement.close();
    }

    public static void close(Connection connection) throws SQLException {
//        try (Statement statement = connection.createStatement()) {
//            statement.executeUpdate("backup to alt-index.db");
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE INDEX start_idx ON altIndex(start)");
            statement.execute("CREATE INDEX start_idx ON altIndex(goal)");
        }
//        }
        connection.close();
    }
}

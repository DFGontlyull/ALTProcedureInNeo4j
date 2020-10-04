package kr.kdelab.neo4jalt;

import com.carrotsearch.hppc.IntObjectScatterMap;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.SCHEMA;

public class BulkInsertProc {

    @Context
    public GraphDatabaseAPI api;

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;





    @Procedure(name = "kde.CreateNode", mode = SCHEMA)
    public Stream<ImportResult> createNode(@Name("label") String label, @Name("nodeSize") Number nodeSize) throws Exception {
        ArrayList<ImportResult> ret = new ArrayList<>();
//        IntObjectScatterMap<Node> intLongScatterMap = new IntObjectScatterMap<>(nodeSize.intValue());

//        db.execute("USING PERIODIC COMMIT 10000");

//        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(path))) {
        int sz = nodeSize.intValue();
        try (Transaction nodeTx = db.beginTx()) {
            for (int i = 1; i <= sz; i++) {
                Node node = db.createNode(Label.label(label));
                node.setProperty("name", i);
//                intLongScatterMap.put(i, node);
            }
//                db.execute("COMMIT");
            nodeTx.success();
        }


//            Transaction relationTx = db.beginTx();
//            String line;
//            int relationPrCount = 0;
//            while ((line = bufferedReader.readLine()) != null) {
//
////                p sp 1070376 2712798
////                c graph contains 1070376 nodes and 2712798 arcs
////                        c
////                a 1 2 35469
////                a 2 1 35469
//                if (line.startsWith("a")) {
//                    int idx = 0;
//                    int pos = 0, end;
//                    int s = 0, e = 0;
////                    int e;
//                    double w = 0;
//                    while ((end = line.indexOf(' ', pos)) >= 0) {
//                        String d = line.substring(pos, end);
//                        switch (idx) {
//                            case 1:
//                                s = Integer.parseInt(d);
//                                break;
//                            case 2:
//                                e = Integer.parseInt(d);
//                                break;
//                            case 3:
//                                w = Double.parseDouble(d) / 100;
//                                break;
//                        }
//                        pos = end + 1;
//                        idx++;
//                    }
//                    if (s != 0 && e != 0) {
//                        relationPrCount++;
//                        intLongScatterMap.get(s).createRelationshipTo(intLongScatterMap.get(e), RelationshipType.withName(relLabel)).setProperty("weight", w);
//
//                        if (relationPrCount % 1000 == 0) {
//                            relationTx.success();
//                            relationTx.close();
//                            relationTx = db.beginTx();
////                            db.execute("COMMIT");
//                        }
//
//                        if (relationPrCount % 1000 == 0)
//                            log.error(String.valueOf(relationPrCount));
//                    }
//                }
//            }
//            relationTx.success();
//            relationTx.close();
//        }


        return ret.stream();
    }


    @Procedure(name = "kde.CreateNode", mode = SCHEMA)
    public Stream<ImportResult> createNodes(@Name("nodeLabel") String nodeLabel, @Name("relLabel") String relLabel, @Name("nodePath") String nodePath, @Name("relPath") String relPath) throws Exception {
        ArrayList<ImportResult> ret = new ArrayList<>();
        HashSet<Integer> nodeList = new HashSet<>();
        ArrayList<ArrayList<String>> tupleList = new ArrayList<>();
        ArrayList<String> trimmedStr = new ArrayList<>();

        String nodeLine = "";
        log.warn(String.valueOf("start procedure"));

        BufferedReader bufferedReader = new BufferedReader(new FileReader(nodePath));


        while ((nodeLine = bufferedReader.readLine()) != null) {
            log.warn(String.valueOf(nodeLine));

            String[] str = {};
            str = nodeLine.split(",");
//            String[] trimmedStr = {};

            for(int t=0; t< str.length; t++){
                trimmedStr.add(str[t].trim());

//                if(t==0)
//                    nodeList.add(Integer.parseInt(str[t].trim()));
                log.warn(String.valueOf("strs = " + str[t].trim()));
            }
//            for(String strs : str){
//                trimmedStr[index++] = strs.trim();
//                if(index==1) {
//                    nodeList.add(Integer.parseInt(strs));
//                }
//                log.warn(String.valueOf("strs = " + strs));
//
//            }
            tupleList.add(trimmedStr);
        }
        ArrayList<String> strarr = new ArrayList<>();

        try (Transaction nodeTx = db.beginTx()) {
            for (int i = 0; i < tupleList.size(); i++) {
                Node node = db.createNode(Label.label(nodeLabel));
                strarr = tupleList.get(i);
                node.setProperty("room", strarr.get(0));
                node.setProperty("building", strarr.get(1));
                node.setProperty("properties1", strarr.get(2));

//                if(strarr.size() >= 3){
//                    for(int j=1; j<= strarr.size() -2 ; j++){
//                        node.setProperty("properties" + (j+1), strarr.get(j+2));
//                    }
//                }
                log.warn(String.valueOf("i = " + i));
//                intLongScatterMap.put(i, node);
            }
//                db.execute("COMMIT");
            nodeTx.success();
        }catch (Exception e) {
            throw new Exception(Util.toString(e));
        }

        return ret.stream();
    }

//    @Procedure(name = "kde.CreateRelation", mode = SCHEMA)
//    public Stream<ImportResult> createRelations(@Name("relLabel") String relLabel, @Name("nodePath") String nodePath, @Name("relPath") String relPath) throws Exception {
//        IntObjectScatterMap<Node> nodeIntObjectScatterMap = new IntObjectScatterMap<>(tupleList.size());
//        RelationshipType relationshipType = RelationshipType.withName(relLabel);
//
//        bufferedReader = new BufferedReader(new FileReader(relPath));
//
//        Transaction relationTx = db.beginTx();
//        String relLine;
//        int relationPrCount = 0;
//
//        while ((relLine = bufferedReader.readLine()) != null) {
//            log.warn(String.valueOf(relLine));
//
//            int s = 0, e = 0;
//
//            Double w = null;
//            String[] str = {};
//            str = relLine.split(",");
//            ArrayList<String> trimmedStr2  = new ArrayList();
//
//            for(int t = 0; t<str.length; t++){
//                trimmedStr2.add(str[t].trim());
//                log.warn(String.valueOf("strs = " + str[t].trim()));
//            }
//
//            s = Integer.parseInt(trimmedStr2.get(0));
//            e = Integer.parseInt(trimmedStr2.get(1));
//            w = Double.parseDouble(trimmedStr2.get(2));
//            log.warn(String.valueOf("w = " + w));
//
//            if (s != 0 && e != 0) {
//                relationPrCount++;
//                nodeIntObjectScatterMap.get(s).createRelationshipTo(nodeIntObjectScatterMap.get(e), relationshipType).setProperty("weight", w);
//
//                if (relationPrCount % 100 == 0) {
//                    log.warn(String.valueOf(relationPrCount));
//                }
//
//                if (relationPrCount % 500 == 0) {
//                    relationTx.success();
//                    relationTx.close();
//
//                    relationTx = db.beginTx();
//                }
//            }
//        }
//
//        return ret.stream();
//    }


    @Procedure(name = "kde.CreateNodeAndRelation", mode = SCHEMA)
    public Stream<ImportResult> createNode(@Name("nodeLabel") String nodeLabel, @Name("relLabel") String relLabel, @Name("nodePath") String nodePath, @Name("relPath") String relPath) throws Exception {
        ArrayList<ImportResult> ret = new ArrayList<>();
        HashSet<Integer> nodeList = new HashSet<>();
        ArrayList<ArrayList<String>> tupleList = new ArrayList<>();
        ArrayList<String> trimmedStr = new ArrayList<>();

        String nodeLine = "";
        log.warn(String.valueOf("start procedure"));

        BufferedReader bufferedReader = new BufferedReader(new FileReader(nodePath));


        while ((nodeLine = bufferedReader.readLine()) != null) {
            log.warn(String.valueOf(nodeLine));

            String[] str = {};
            str = nodeLine.split(",");
//            String[] trimmedStr = {};

            for(int t=0; t< str.length; t++){
                trimmedStr.add(str[t].trim());

//                if(t==0)
//                    nodeList.add(Integer.parseInt(str[t].trim()));
                log.warn(String.valueOf("strs = " + str[t].trim()));
            }
//            for(String strs : str){
//                trimmedStr[index++] = strs.trim();
//                if(index==1) {
//                    nodeList.add(Integer.parseInt(strs));
//                }
//                log.warn(String.valueOf("strs = " + strs));
//
//            }
            tupleList.add(trimmedStr);
        }
        ArrayList<String> strarr = new ArrayList<>();

        try (Transaction nodeTx = db.beginTx()) {
            for (int i = 0; i < tupleList.size(); i++) {
                Node node = db.createNode(Label.label(nodeLabel));
                strarr = tupleList.get(i);
                node.setProperty("room", strarr.get(0));
                node.setProperty("building", strarr.get(1));
                node.setProperty("properties1", strarr.get(2));

//                if(strarr.size() >= 3){
//                    for(int j=1; j<= strarr.size() -2 ; j++){
//                        node.setProperty("properties" + (j+1), strarr.get(j+2));
//                    }
//                }
                log.warn(String.valueOf("i = " + i));
//                intLongScatterMap.put(i, node);
            }
//                db.execute("COMMIT");
            nodeTx.success();
        }catch (Exception e) {
            throw new Exception(Util.toString(e));
        }




        IntObjectScatterMap<Node> nodeIntObjectScatterMap = new IntObjectScatterMap<>(tupleList.size());
        RelationshipType relationshipType = RelationshipType.withName(relLabel);

        bufferedReader = new BufferedReader(new FileReader(relPath));

        Transaction relationTx = db.beginTx();
        String relLine;
        int relationPrCount = 0;

        while ((relLine = bufferedReader.readLine()) != null) {
            log.warn(String.valueOf(relLine));

            int s = 0, e = 0;

            Double w = null;
            String[] str = {};
            str = relLine.split(",");
            ArrayList<String> trimmedStr2  = new ArrayList();

            for(int t = 0; t<str.length; t++){
                trimmedStr2.add(str[t].trim());
                log.warn(String.valueOf("strs = " + str[t].trim()));
            }

            s = Integer.parseInt(trimmedStr2.get(0));
            e = Integer.parseInt(trimmedStr2.get(1));
            w = Double.parseDouble(trimmedStr2.get(2));
            log.warn(String.valueOf("w = " + w));

            if (s != 0 && e != 0) {
                relationPrCount++;
                nodeIntObjectScatterMap.get(s).createRelationshipTo(nodeIntObjectScatterMap.get(e), relationshipType).setProperty("weight", w);

                if (relationPrCount % 100 == 0) {
                    log.warn(String.valueOf(relationPrCount));
                }

                if (relationPrCount % 500 == 0) {
                    relationTx.success();
                    relationTx.close();

                    relationTx = db.beginTx();
                }
            }
        }

        return ret.stream();
    }

    @Procedure(name = "kde.ImportGr", mode = SCHEMA)
    public Stream<ImportResult> importGr(@Name("label") String label, @Name("path") String path, @Name("relLable") String relLabel, @Name("nodeSize") Number nodeSize) throws Exception {
        ArrayList<ImportResult> ret = new ArrayList<>();
        try {
            IntObjectScatterMap<Node> nodeIntObjectScatterMap = new IntObjectScatterMap<>(nodeSize.intValue());
            Label label1 = Label.label(label);
            RelationshipType relationshipType = RelationshipType.withName(relLabel);

            for (ResourceIterator<Node> it = db.findNodes(label1); it.hasNext(); ) {
                Node i = it.next();
                nodeIntObjectScatterMap.put((Integer) i.getProperty("name"), i);
            }
//        db.execute("USING PERIODIC COMMIT 10000");

            try (BufferedReader bufferedReader = new BufferedReader(new FileReader(path))) {
//            int sz = nodeSize.intValue();
//            try (Transaction nodeTx = db.beginTx()) {
//                for (int i = 1; i <= sz; i++) {
//                    Node node = db.createNode(Label.label(label));
//                    node.setProperty("name", sz);
//                    intLongScatterMap.put(i, node);
//                }
////                db.execute("COMMIT");
//                nodeTx.success();
//            }


                Transaction relationTx = db.beginTx();
                String line;
                int relationPrCount = 0;
                while ((line = bufferedReader.readLine()) != null) {

//                p sp 1070376 2712798
//                c graph contains 1070376 nodes and 2712798 arcs
//                        c
//                a 1 2 35469
//                a 2 1 35469
                    if (line.startsWith("a")) {
                        int idx = 0;
                        int pos = 0, end;
                        int s = 0, e = 0;
//                    int e;
                        Double w = null;
                        while ((end = line.indexOf(' ', pos)) >= 0) {
                            String d = line.substring(pos, end);

                            switch (idx) {
                                case 1:
                                    s = Integer.parseInt(d);
                                    break;
                                case 2:
                                    e = Integer.parseInt(d);
                                    break;
//                                case 3:
//                                    w = Double.parseDouble(d);
//                                    break;
                            }
                            pos = end + 1;
                            idx++;
                        }
                        w = Double.parseDouble(line.substring(pos, line.length()));
//                        log.error(line);
//                        log.error(String.valueOf(s));
//                        log.error(String.valueOf(e));
//                        log.error(String.valueOf(w));

                        if (s != 0 && e != 0) {
                            relationPrCount++;
//                            db.execute("MATCH (S),(E) WHERE S.name=" + s + " AND E.name=" + e + " CREATE (S)-[:RELTYPE { weight: " + w + " }]->(E) ");
                            nodeIntObjectScatterMap.get(s).createRelationshipTo(nodeIntObjectScatterMap.get(e), relationshipType).setProperty("weight", w);
//                            db.findNode(label1, "name", s)

                            if (relationPrCount % 500 == 0) {
                                relationTx.success();
                                relationTx.close();
//                            transaction.success();

                                relationTx = db.beginTx();
//                            db.execute("COMMIT");
                            }

                            if (relationPrCount % 10000 == 0) {
                                log.warn(String.valueOf(relationPrCount));
                            }

//                        if (relationPrCount % 1000 == 0)
                        }
                    }
                }
                relationTx.success();
                relationTx.close();
//            transaction.success();
            }


        } catch (Exception e) {
            throw new Exception(Util.toString(e));
        }
        return ret.stream();
    }

    public static class ImportResult {
        public Number edges;
        public Number nodes;

        public ImportResult(int edges, int nodes) {
            this.edges = edges;
            this.nodes = nodes;
        }
    }
}

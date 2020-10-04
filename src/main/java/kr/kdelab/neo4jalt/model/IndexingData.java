package kr.kdelab.neo4jalt.model;

public class IndexingData {
    public long start;
    public long end;
    public Double cost;
    public Integer areaId;

    public IndexingData(long start, long end, Double cost, Integer areaId) {
        this.start = start;
        this.end = end;
        this.cost = cost;
        this.areaId = areaId;
    }
}

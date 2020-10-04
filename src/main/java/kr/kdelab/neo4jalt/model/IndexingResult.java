package kr.kdelab.neo4jalt.model;

public class IndexingResult {
    public long areaId;
    public long size;

    public IndexingResult(long areaId, long size) {
        this.areaId = areaId;
        this.size = size;
    }
}

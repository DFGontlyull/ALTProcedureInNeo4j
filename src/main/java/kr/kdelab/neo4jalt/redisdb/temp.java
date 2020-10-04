package kr.kdelab.neo4jalt.redisdb;//package kr.kdelab.neo4jalt.redisdb;
//
//import java.text.SimpleDateFormat;
//import java.util.*;
//import redis.clients.jedis.Jedis;
//
///**
// * 해시데이터로 처리.
// * 날자별 저장, 모든 날자의 방문횟수 조회.
// */
//public class temp {
//    private JedisHelper jedisHelper;
//
//    private Jedis jedis;
//
//    private static final String KEY_EVENT_DAILY_CLICK_TOTAL = "event:daily:click:total:hash";
//
//    private static final String KEY_EVENT_CLICK_CLICK = "event:daily:click:hash:";
//
//    /**
//     * 방문횟수 처리를 위한 클래스 생성자.
//     * @param jedisHelper 제디스 도우미 객체
//     */
//    public temp(JedisHelper jedisHelper) {
//        this.jedisHelper = jedisHelper;
//        this.jedis = this.jedisHelper.getConnection();
//    }
//
//    /**
//     * 이벤트 아이디에 해당하는 날자별 방문횟수와 날자별 전체 방문횟수를 증가시킨다.
//     * @param eventId 이벤트 아이디
//     * @return 이벤트 페이지 방문횟수
//     */
//    public Long addVisit(String eventId) {
//        this.jedis.hincrBy(KEY_EVENT_DAILY_CLICK_TOTAL, getToday(), 1);
//        return this.jedis.hincrBy(KEY_EVENT_CLICK_CLICK + eventId+":", getToday(), 1);
//    }
//
//    /**
//     * 이벤트 페이지에 대한 모든 날자별 방문횟수를 조회한다.
//     * @return 전체 이벤트 페이지 방문횟수
//     */
//    public SortedMap<String, String> getVisitCountByDailyTotal() {
//        SortedMap<String, String> result = new TreeMap<String, String>();
//        result.putAll(this.jedis.hgetAll(KEY_EVENT_DAILY_CLICK_TOTAL));
//        return result;
//    }
//
//    /**
//     * 이벤트 아이디에 해당하는 모든 날자의 방문횟수를 조회한다.
//     * @param eventId 요청된 이벤트아이디
//     * @return 날자로 정렬된 방문횟수 목록
//     */
//    public SortedMap<String, String> getVisitCountByDaily(String eventId) {
//        SortedMap<String, String> result = new TreeMap<String, String>();
//        result.putAll(this.jedis.hgetAll(KEY_EVENT_CLICK_CLICK + eventId));
//        return result;
//    }
//
//    private String getToday() {
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
//        return sdf.format(new Date());
//    }
//}


import kr.kdelab.neo4jalt.AStarProc;

public class temp{
    public static void main(String[] args) {

        AStarProc a = new AStarProc();
    }
}
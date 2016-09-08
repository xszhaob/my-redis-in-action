package chapter05;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.List;

/**
 * author:xszhaobo
 * <p/>
 * date:2016/9/8
 * <p/>
 * package_name:chapter05
 * <p/>
 * project: my-redis-in-action
 * 记录和计算访问时长
 */
public class AccessTime {


    private void accessTime(Jedis conn,String context) {
        long start = System.currentTimeMillis();
        processSomething();
        long timeConsuming = System.currentTimeMillis() - start;

        List<Long> stats = new UpdateStats().updateStats(conn, context, "accessTime", (int) timeConsuming, 1000);
        long average = stats.get(1)/stats.get(0);
        Transaction trans = conn.multi();
        trans.zadd("slowest:accessTime",average,context);
        trans.zremrangeByRank("slowest:accessTime",0,-101);
    }


    private void processSomething() {
        System.out.println("process something.");
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

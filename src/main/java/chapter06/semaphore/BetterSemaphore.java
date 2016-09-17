package chapter06.semaphore;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.ZParams;

import java.util.List;
import java.util.UUID;

/**
 * author:xszhaobo
 * <p/>
 * date:2016/9/17
 * <p/>
 * package_name:chapter06.semaphore
 * <p/>
 * project: my-redis-in-action
 */
public class BetterSemaphore {

    /**
     * 获取信号量
     *
     * @param conn    redis连接
     * @param semName 信号量名称
     * @param limit   信号量限制数
     * @param timeOut 信号量过期时间
     * @return 如果获取信号量成功则返回信号量id，否则返回null
     */
    private String acquireFairSemaphore(Jedis conn, String semName, int limit, long timeOut) {
        String id = UUID.randomUUID().toString();
        String czset = semName + ":owner";
        String ctr = semName + ":counter";

        // 删除过期信号量
        long now = System.currentTimeMillis();
        Transaction trans = conn.multi();
        trans.zremrangeByScore(semName, 0, now - timeOut);
        ZParams zParams = new ZParams();
        zParams.weightsByDouble(1, 0);
        trans.zinterstore(czset, zParams, czset, semName);
        trans.incr(ctr);
        List<Object> exec = trans.exec();
        long counter = (Long) (exec.get(exec.size() - 1));

        // 尝试获取信号量
        trans = conn.multi();
        trans.zadd(semName, now, id);
        trans.zadd(czset, counter, id);
        trans.zrank(czset, id);
        List<Object> result = trans.exec();
        long index = (Long) (result.get(result.size() - 1));
        // 获取信号量成功
        if (index < limit) {
            return id;
        }

        // 获取信号量失败
        trans = conn.multi();
        trans.zrem(semName, id);
        trans.zrem(czset, id);
        trans.exec();
        return null;
    }

    /**
     * 释放信号量
     *
     * @param conn    redis连接
     * @param semName 信号量名称
     * @param id      信号量id
     * @return true如果删除成功，否则返回失败。
     * 如果信号量在释放前已经过期而被释放，那么也会返回false
     */
    private boolean releaseFairSemaphore(Jedis conn, String semName, String id) {
        String czset = semName + ":owner";
        Transaction trans = conn.multi();
        trans.zrem(semName, id);
        trans.zrem(czset, id);
        List<Object> exec = trans.exec();
        return (Long) (exec.get(0)) == 1;
    }
}

package chapter06.semaphore;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.List;
import java.util.UUID;

/**
 * author:xszhaobo
 * <p/>
 * date:2016/9/16
 * <p/>
 * package_name:chapter06.semaphore
 * <p/>
 * project: my-redis-in-action
 * 利用redis实现一个简单的信号量
 */
public class BaseSemaphore {

    /**
     * 获取一个信号量
     *
     * @param conn    redis连接
     * @param semName 信号量名称
     * @param limit   信号量限制数
     * @param timeOut 获取的信号量过期时间
     * @return 信号量标识
     */
    private String acquireSemaphore(Jedis conn, String semName, int limit, long timeOut) {
        String member = UUID.randomUUID().toString();
        long now = System.currentTimeMillis();

        Transaction trans = conn.multi();
        // 先删除过期的信号量
        trans.zremrangeByScore(semName, 0, now - timeOut);
        trans.zadd(semName, now, member);
        trans.zrank(semName, member);
        List<Object> exec = trans.exec();
        if (exec != null && (Long) (exec.get(exec.size() - 1)) < limit) {
            return member;
        }
        return null;
    }

    /**
     * 删除信号量
     *
     * @param conn    redis连接
     * @param semName 信号量名称
     * @param id      信号量标识
     */
    private void releaseSemaphore(Jedis conn, String semName, String id) {
        conn.zrem(semName, id);
    }
}

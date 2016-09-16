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


    private String acquireSemaphore(Jedis conn,String semName,int limit,long timeOut) {
        String member = UUID.randomUUID().toString();
        long now = System.currentTimeMillis();

        Transaction trans = conn.multi();
        // 先删除过期的信号量
        trans.zremrangeByScore(semName,0,now - timeOut);
        trans.zadd(semName,now,member);
        trans.zrank(semName,member);
        List<Object> exec = trans.exec();
        if (exec != null && (Long)(exec.get(exec.size() - 1)) < limit) {
            return member;
        }
        return null;
    }


}
